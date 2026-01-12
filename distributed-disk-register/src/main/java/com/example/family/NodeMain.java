package com.example.family;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.example.family.SetGetCommand.Command;
import com.example.family.SetGetCommand.CommandParser;
import com.example.family.SetGetCommand.DataStore;
import com.example.family.SetGetCommand.GetCommand;
import com.example.family.SetGetCommand.SetCommand;

import family.ChatMessage;
import family.Empty;
import family.FamilyServiceGrpc;
import family.FamilyView;
import family.MessageId;
import family.NodeInfo;
import family.StorageServiceGrpc;
import family.StoreResult;
import family.StoredMessage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;

public class NodeMain {

    private static final int START_PORT = 5555;
    private static final int PRINT_INTERVAL_SECONDS = 10;
    // SET/GET verilerini tuttuğumuz Map
    private static final DataStore STORE = new DataStore();
    private static final MessageReplicaTracker REPLICA_TRACKER = new MessageReplicaTracker();

    public static void main(String[] args) throws Exception {
        ToleranceConfig.loadConfig();

        String host = "127.0.0.1";
        int port = findFreePort(START_PORT); // 5555 ve sonrası için boş olan ilk portu verir

        // 1. Ana klasör
        File parentDir = new File("messages");
        if (!parentDir.exists()) {
            parentDir.mkdirs();
        }
        // 2. Node klasörü (messages/messages_5555)
        currentNodeDir = new File(parentDir, "messages_" + port);
        if (!currentNodeDir.exists()) {
            currentNodeDir.mkdirs();
        }
        System.out.println("Aktif Disk Klasörü: " + currentNodeDir.getPath());


        NodeInfo self = NodeInfo.newBuilder() // Üyenin kendisi
                .setHost(host)
                .setPort(port)
                .build();

        NodeRegistry registry = new NodeRegistry();
        FamilyServiceImpl service = new FamilyServiceImpl(registry, self);

        StorageServiceImpl storageService = new StorageServiceImpl(STORE, port);

        Server server = ServerBuilder
                .forPort(port)
                .addService(service)
                .addService(storageService)
                .build()
                .start();

        System.out.printf("Node started on %s:%d%n", host, port);

        // Eğer bu ilk node ise (port 5555), TCP 6666'da text dinlesin
        if (port == START_PORT) {
            startLeaderTextListener(registry, self);
        }

        discoverExistingNodes(host, port, registry, self);
        startFamilyPrinter(registry, self);
        startHealthChecker(registry, self);

        server.awaitTermination();

    }

    // Lider node için TCP 6666'da text dinleme
    // Lider node için TCP dinleyici başlatan metot.
    // registry → family’de kimler var bilgisi
    // self → bu node’un (liderin) bilgisi (host, port)
    private static void startLeaderTextListener(NodeRegistry registry, NodeInfo self) {
        // Sadece lider (5555 portlu node) bu methodu çağırmalı
        // Serversocket accept bloklayıcıdır ana thread’i etkilememesi için ayrı bir
        // thread’de çalıştırıyoruz
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(6666)) { // TCP 6666 portunu açıyor
                System.out.printf("Leader listening for text on TCP %s:%d%n",
                        self.getHost(), 6666);

                while (true) {
                    Socket client = serverSocket.accept(); // Yeni bir client bağlandığında kabul et
                    // Her client için ayrı bir thread açılıyor
                    // O Thread Client’tan satır satır komut okur
                    // SET / GET / STATS işler
                    // Cevabı client’a yazar bu sayede bir client yavaş olsa bile diğerleri
                    // etkilenmez
                    new Thread(() -> handleClientTextConnection(client, registry, self)).start();
                }

            } catch (IOException e) {
                System.err.println("Error in leader text listener: " + e.getMessage());
            }
        }, "LeaderTextListener").start();
    }

    private static void handleClientTextConnection(Socket client,
            NodeRegistry registry,
            NodeInfo self) {

        // Yeni bir TCP client bağlandığında burası çalışır
        // client = 6666 portuna bağlanan test client (HaToKuSeClient gibi)
        // getRemoteSocketAddress(): client’ın IP ve port bilgisini verir
        System.out.println("New TCP client connected: " + client.getRemoteSocketAddress());

        // try-with-resources:
        // Client kapandığında reader otomatik kapanır
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(client.getInputStream()))) {

            String line;

            // Client bağlantısı açık olduğu sürece satır satır komut okur
            // Client bağlantıyı kapatırsa readLine() null döner ve döngü biter
            while ((line = reader.readLine()) != null) {

                // Gelen komutu temizle (baş-son boşluklar)
                String text = line.trim();

                // Boş satır geldiyse yok say
                if (text.isEmpty()) {
                    continue;
                }

                // Log: TCP üzerinden ne geldiğini ekrana bas
                System.out.println(" Received from TCP: " + text);

                // ÖZEL KOMUT: STATS
                // Bu komut sistemin genel yük durumunu ister
                if (text.equalsIgnoreCase("STATS")) {

                    // Replica tracker üzerinden yük istatistiklerini hesapla
                    String statsReport = calculateLoadStats(registry);

                    // Client'a cevabı yaz
                    PrintWriter writer = new PrintWriter(client.getOutputStream(), true);
                    writer.println(statsReport);

                    // STATS sadece rapor ister
                    // Replication veya broadcast yapılmaz
                    continue;
                }

                try {
                    // 1) Gelen text komutu parse et (SET / GET / vs.)
                    Command cmd = CommandParser.parse(text);

                    String result;

                    // === SET KOMUTU ===
                    if (cmd instanceof SetCommand setCmd) {

                        // SET <id> <value>
                        int messageId = setCmd.getKey();
                        String messageText = setCmd.getValue();

                        // Lokal memory/disk yazımı kapalı (distributed sistem kullanılıyor)

                        // Mesajı family üyelerine replike et
                        result = replicateToMembers(registry, self, messageId, messageText);

                    }
                    // === GET KOMUTU ===
                    else if (cmd instanceof GetCommand getCmd) {

                        int messageId = getCmd.getKey();

                        // Mesajı kendi diskinden değil,
                        // daha önce replike edilmiş üyelerden almaya çalış
                        String value = retrieveFromMembers(messageId);

                        // Eğer hiçbir node'da bulunamadıysa
                        if (value == null) {
                            result = "NOT_FOUND";
                        }
                        // Bulunduysa OK ile birlikte değeri dön
                        else {
                            result = "OK " + value;
                        }

                    }
                    // === BİLİNMEYEN KOMUT ===
                    else {
                        result = "ERROR: Unknown command";
                    }

                    // Client'a sonucu gönder (SET/GET cevabı)
                    PrintWriter writer = new PrintWriter(client.getOutputStream(), true);
                    writer.println(result);

                    // Bu komutu chat mesajı gibi family üyelerine duyurmak için
                    long ts = System.currentTimeMillis();
                    ChatMessage msg = ChatMessage.newBuilder()
                            .setText(text)
                            .setFromHost(self.getHost())
                            .setFromPort(self.getPort())
                            .setTimestamp(ts)
                            .build();

                    // Tüm family üyelerine bu komutu broadcast et
                    broadcastToFamily(registry, self, msg);

                } catch (IllegalArgumentException e) {
                    // Komut parse edilemezse (format yanlışsa)
                    // Hata loglanır
                    System.out.println("ERROR: " + e.getMessage());
                }

            }

        } catch (IOException e) {
            // TCP okuma/yazma sırasında hata olursa buraya düşer
            System.err.println("TCP client handler error: " + e.getMessage());

        } finally {
            // Client bağlantısını düzgün şekilde kapat
            try {
                client.close();
            } catch (IOException ignored) {
            }
        }
    }

    private static void broadcastToFamily(NodeRegistry registry,
            NodeInfo self,
            ChatMessage msg) {

        // Registry’den (family) tüm node’ların anlık bir kopyasını alıyoruz
        // snapshot → thread-safe, sadece okuma amaçlı liste
        List<NodeInfo> members = registry.snapshot();

        // Family’deki HER node için döngü
        for (NodeInfo n : members) {

            // === 1) KENDİMİZE TEKRAR GÖNDERMEYELİM ===
            // Eğer listedeki node bu node’un kendisiyse (host + port aynıysa)
            // mesajı atlamamız lazım
            if (n.getHost().equals(self.getHost()) && n.getPort() == self.getPort()) {
                continue; // bu node’u geç, diğerine bak
            }

            ManagedChannel channel = null;
            try {
                // === 2) KARŞI NODE’A gRPC KANALI AÇ ===
                // n.getHost() → hedef node’un IP’si
                // n.getPort() → hedef node’un gRPC portu
                channel = ManagedChannelBuilder
                        .forAddress(n.getHost(), n.getPort())
                        .usePlaintext() // TLS yok, düz TCP (local test için)
                        .build();

                // === 3) BLOCKING gRPC STUB OLUŞTUR ===
                // BlockingStub → bu çağrı bitene kadar thread bekler
                FamilyServiceGrpc.FamilyServiceBlockingStub stub = FamilyServiceGrpc.newBlockingStub(channel);

                // === 4) receiveChat RPC ÇAĞRISI ===
                // Bu satır:
                // - Karşı node’daki FamilyServiceImpl.receiveChat() metodunu çağırır
                // - msg (ChatMessage) karşı tarafa gönderilir
                // - Karşı taraf Empty döner
                stub.receiveChat(msg);

                // === 5) BAŞARI LOG’U ===
                System.out.printf("Broadcasted message to %s:%d%n",
                        n.getHost(), n.getPort());

            } catch (Exception e) {
                // === 6) HATA DURUMU ===
                // Node kapalı olabilir
                // Network timeout olabilir
                // gRPC exception olabilir
                System.err.printf("Failed to send to %s:%d (%s)%n",
                        n.getHost(), n.getPort(), e.getMessage());
            } finally {
                // === 7) KANALI KAPAT ===
                // Her node için açılan channel mutlaka kapatılmalı
                // Yoksa port / connection leak olur
                if (channel != null) {
                    channel.shutdownNow();
                }
            }
        }
    }

    private static int findFreePort(int startPort) {
        int port = startPort;
        while (true) {
            try (ServerSocket ignored = new ServerSocket(port)) {
                return port;
            } catch (IOException e) {
                port++;
            }
        }
    }

    private static void discoverExistingNodes(String host,
            int selfPort,
            NodeRegistry registry,
            NodeInfo self) {

        for (int port = START_PORT; port < selfPort; port++) {
            ManagedChannel channel = null;
            try {
                channel = ManagedChannelBuilder
                        .forAddress(host, port)
                        .usePlaintext()
                        .build();

                FamilyServiceGrpc.FamilyServiceBlockingStub stub = FamilyServiceGrpc.newBlockingStub(channel);

                // Karşılıklı tanışma
                FamilyView view = stub.join(self);
                registry.addAll(view.getMembersList());

                System.out.printf("Joined through %s:%d, family size now: %d%n",
                        host, port, registry.snapshot().size());

            } catch (Exception ignored) {
            } finally {
                if (channel != null) {
                    channel.shutdownNow();
                }
            }
        }
    }

    private static void startFamilyPrinter(NodeRegistry registry, NodeInfo self) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        scheduler.scheduleAtFixedRate(() -> {
            List<NodeInfo> members = registry.snapshot();
            System.out.println("======================================");
            System.out.printf("Node Status [%s:%d]%n", self.getHost(), self.getPort());
            System.out.println("Time: " + LocalDateTime.now());

            // Üyeler periyodik olarak kendi disklerinde kaç mesaj sakladıklarını
            // bastırmalıdır.
            System.out.println("Yerelde Kaydedilen Mesajlar: " + STORE.getSize());

            System.out.println("Family Members:");
            for (NodeInfo n : members) {
                boolean isMe = n.getHost().equals(self.getHost()) && n.getPort() == self.getPort();
                System.out.printf(" - %s:%d%s%n",
                        n.getHost(),
                        n.getPort(),
                        isMe ? " (me)" : "");
            }

            // Lider, periyodik olarak sistemde toplam kaç mesaj saklandığını bastırmalıdır.
            if (self.getPort() == START_PORT) {
                System.out.println("\n--- LEADER REPORT ---");
                // Mevcut calculateLoadStats metodunu kullanarak genel durumu basıyoruz
                System.out.print(calculateLoadStats(registry));
            }

            System.out.println("======================================");
        }, 3, PRINT_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    private static void startHealthChecker(NodeRegistry registry, NodeInfo self) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        scheduler.scheduleAtFixedRate(() -> {
            List<NodeInfo> members = registry.snapshot();

            for (NodeInfo n : members) {
                // Kendimizi kontrol etmeyelim
                if (n.getHost().equals(self.getHost()) && n.getPort() == self.getPort()) {
                    continue;
                }

                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder
                            .forAddress(n.getHost(), n.getPort())
                            .usePlaintext()
                            .build();

                    FamilyServiceGrpc.FamilyServiceBlockingStub stub = FamilyServiceGrpc.newBlockingStub(channel);

                    // Ping gibi kullanıyoruz: cevap bizi ilgilendirmiyor,
                    // sadece RPC'nin hata fırlatmaması önemli.
                    stub.getFamily(Empty.newBuilder().build());

                } catch (Exception e) {
                    // Bağlantı yok / node ölmüş → listeden çıkar
                    System.out.printf("Node %s:%d unreachable, removing from family%n",
                            n.getHost(), n.getPort());
                    registry.remove(n);
                } finally {
                    if (channel != null) {
                        channel.shutdownNow();
                    }
                }
            }

        }, 5, 10, TimeUnit.SECONDS); // 5 sn sonra başla, 10 sn'de bir kontrol et
    }

    private static File currentNodeDir;

    // private static void writeMessageToDisk(int id, String msg) { // String id ->
    // int id
    // File file = new File(currentNodeDir, id + ".msg");
    // try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
    // bw.write(msg);
    // } catch (IOException e) {
    // e.printStackTrace();
    // }
    // }

    // private static String readMessageFromDisk(int id) { // String id -> int id
    // File file = new File(currentNodeDir, id + ".msg");
    // if (!file.exists()) {
    // return null;
    // }

    // try (BufferedReader br = new BufferedReader(new FileReader(file))) {
    // return br.readLine();
    // } catch (IOException e) {
    // e.printStackTrace();
    // return null;
    // }
    // }

    private static String replicateToMembers(NodeRegistry registry, NodeInfo self,
            int messageId, String messageText) {

        // === 1) TOLERANCE DEĞERİNİ AL ===
        // Kaç kopya (replica) tutulması gerektiğini söyler
        int tolerance = ToleranceConfig.getTolerance();

        // Family’deki tüm node’ların anlık listesi
        List<NodeInfo> allMembers = registry.snapshot();

        // === 2) SADECE DİĞER NODE’LARI SEÇ ===
        // Kendi node’umuzu listeden çıkarıyoruz
        List<NodeInfo> eligibleMembers = new ArrayList<>();
        for (NodeInfo member : allMembers) {
            if (!(member.getHost().equals(self.getHost())
                    && member.getPort() == self.getPort())) {
                eligibleMembers.add(member);
            }
        }

        // === 3) SADECE LİDER VARSA ===
        // Replication yapacak başka node yoksa
        if (eligibleMembers.isEmpty()) {
            System.out.println("No members available for replication, only leader exists");
            return "OK (ONLY LEADER)";
        }

        // === 4) YÜK DENGELEME İÇİN SIRALA ===
        // Port numarasına göre sıralama (deterministic seçim)
        Collections.sort(eligibleMembers, Comparator.comparingInt(NodeInfo::getPort));

        // Gerçekte kaç node’a yazacağımız
        int replicasNeeded = Math.min(tolerance, eligibleMembers.size());

        // Mesaj ID’ye göre başlangıç index’i (hash benzeri davranış)
        int startIndex = messageId % eligibleMembers.size();

        // === 5) REPLICA ATANACAK NODE’LARI SEÇ ===
        // Döngüsel (wrap-around) seçim
        List<NodeInfo> selectedMembers = new ArrayList<>();
        for (int i = 0; i < replicasNeeded; i++) {
            int currentIndex = (startIndex + i) % eligibleMembers.size();
            selectedMembers.add(eligibleMembers.get(currentIndex));
        }

        // === 6) SEÇİLEN NODE’LARA MESAJI GÖNDER ===
        int successCount = 0;

        for (NodeInfo member : selectedMembers) {
            ManagedChannel channel = null;
            try {
                // gRPC bağlantısı aç
                channel = ManagedChannelBuilder
                        .forAddress(member.getHost(), member.getPort())
                        .usePlaintext()
                        .build();

                // Blocking stub → store RPC çağrısı
                StorageServiceGrpc.StorageServiceBlockingStub stub = StorageServiceGrpc.newBlockingStub(channel);

                // Diskte saklanacak mesaj
                StoredMessage msg = StoredMessage.newBuilder()
                        .setId(messageId)
                        .setText(messageText)
                        .build();

                // Karşı node’a "store" çağrısı
                StoreResult result = stub.store(msg);

                // Başarılıysa
                if (result.getSuccess()) {
                    // Replica bilgisini liderde kaydet
                    REPLICA_TRACKER.addReplica(messageId, member);
                    successCount++;

                    System.out.printf("Replicated msg %d to %s:%d (LoadBalanced)%n",
                            messageId, member.getHost(), member.getPort());
                }

            } catch (Exception e) {
                // Node kapalı / timeout / network hatası
                System.err.printf("Failed to replicate to %s:%d - %s%n",
                        member.getHost(), member.getPort(), e.getMessage());
            } finally {
                // Kanalı mutlaka kapat
                if (channel != null) {
                    channel.shutdownNow();
                }
            }
        }

        // === 7) SONUÇ ===
        // En az 1 replica bile başarılıysa OK dön (tasarım tercihi)
        if (successCount >= 1) {
            return "OK";
        } else {
            return "ERROR: Replication failed";
        }
    }

    private static String retrieveFromMembers(int messageId) {

        // === 1) BU MESAJIN HANGİ NODE’LARDA OLDUĞUNU AL ===
        List<NodeInfo> members = REPLICA_TRACKER.getMembersForMessage(messageId);

        // Hiç kayıt yoksa
        if (members.isEmpty()) {
            System.out.println("No replica information found for message " + messageId);
            return null;
        }

        // === 2) NODE NODE GEZ, MESAJI BULMAYA ÇALIŞ ===
        for (NodeInfo member : members) {
            ManagedChannel channel = null;
            try {
                // Node’a bağlan
                channel = ManagedChannelBuilder
                        .forAddress(member.getHost(), member.getPort())
                        .usePlaintext()
                        .build();

                // Storage servis stub’ı
                StorageServiceGrpc.StorageServiceBlockingStub stub = StorageServiceGrpc.newBlockingStub(channel);

                // İstenen mesajın ID’si
                MessageId msgId = MessageId.newBuilder()
                        .setId(messageId)
                        .build();

                // Karşı node’dan mesajı iste
                StoredMessage response = stub.retrieve(msgId);

                // Mesaj bulunduysa
                if (response != null && !response.getText().isEmpty()) {
                    System.out.printf("Retrieved message %d from %s:%d%n",
                            messageId, member.getHost(), member.getPort());

                    // İlk bulunan mesajı döndür
                    return response.getText();
                }

            } catch (Exception e) {
                // Node’a ulaşılamadı / hata oldu
                System.err.printf("Failed to retrieve from %s:%d - %s%n",
                        member.getHost(), member.getPort(), e.getMessage());
            } finally {
                // Kanalı kapat
                if (channel != null) {
                    channel.shutdownNow();
                }
            }
        }

        // === 3) HİÇBİR NODE’DAN OKUNAMADI ===
        return null;
    }

    private static String calculateLoadStats(NodeRegistry registry) {

        // REPLICA_TRACKER içinden tüm mesaj → node listesi bilgisini alıyoruz
        // Örnek yapı:
        // messageId -> [node1, node2, node3]
        Map<Integer, List<NodeInfo>> data = REPLICA_TRACKER.getSnapshot();

        // Her node'un kaç mesaj tuttuğunu saymak için map
        // Key: "host:port"
        // Value: o node'daki mesaj sayısı
        Map<String, Integer> nodeCounts = new HashMap<>();

        // =====================================================
        // 1) MESAJ → NODE DAĞILIMINI MEVCUT TABLOYU OKU VE SAY
        // =====================================================
        // Her mesajın hangi node'larda tutulduğunu geziyoruz

        // Diyelim ki sistemin durumu şu:
        // Message 1 → [NodeA, NodeB]
        // Message 2 → [NodeA]
        // Message 3 → [NodeB, NodeC]
        // Bu şunu yapar:
        // Önce [A, B]
        // Sonra [A]
        // Sonra [B, C]
        // Yani mesaj mesaj geziyoruz.
        for (List<NodeInfo> nodes : data.values()) {

            // Bir mesajın tutulduğu her node için
            for (NodeInfo node : nodes) {

                // Node'u benzersiz tanımlamak için "host:port" formatı
                // NodeA → "127.0.0.1:5555"
                // NodeB → "127.0.0.1:5556"
                // NodeC → "127.0.0.1:5557"
                String key = node.getHost() + ":" + node.getPort();

                // O node için mesaj sayısını 1 artır
                // Yoksa 0'dan başlat
                nodeCounts.put(key, nodeCounts.getOrDefault(key, 0) + 1);
            }
        }

        // =====================================================
        // 2) RAPOR METNİNİ OLUŞTUR
        // =====================================================
        StringBuilder sb = new StringBuilder();

        // Başlık
        sb.append("\n=== MESAJ DAĞILIMLARI ===\n");

        // Sistemde toplam kaç farklı mesaj var
        sb.append("Toplam Kaydedilen Mesajlar: ")
                .append(data.size())
                .append("\n");

        // =====================================================
        // 3) TÜM AİLE ÜYELERİNİ LİSTELE
        // =====================================================
        // Registry'deki (bildiğimiz) tüm node'ları alıyoruz
        List<NodeInfo> allMembers = new ArrayList<>(registry.snapshot());

        // Port numarasına göre sıralıyoruz
        // (çıktı düzenli ve okunabilir olsun diye)
        allMembers.sort(Comparator.comparingInt(NodeInfo::getPort));

        // =====================================================
        // 4) HER NODE İÇİN KAÇ MESAJ VAR YAZ
        // =====================================================
        for (NodeInfo member : allMembers) {

            // Aynı "host:port" key formatı
            String key = member.getHost() + ":" + member.getPort();

            // Eğer bu node'da hiç mesaj yoksa 0 yaz
            int count = nodeCounts.getOrDefault(key, 0);

            // Satır satır rapora ekle
            sb.append(String.format(
                    "Node %s -> %d mesaj\n",
                    key,
                    count));
        }

        // =====================================================
        // 5) OLUŞAN RAPORU STRING OLARAK DÖN
        // =====================================================
        return sb.toString();
    }

}

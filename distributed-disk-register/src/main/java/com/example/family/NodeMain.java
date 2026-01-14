package com.example.family;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
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
// DataStore import removed
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
    // STORE Removed
    private static final MessageReplicaTracker REPLICA_TRACKER = new MessageReplicaTracker();

    public static void main(String[] args) throws Exception {
        ToleranceConfig.loadConfig();

        String host = "127.0.0.1";
        String leaderHost = "127.0.0.1";
        int leaderPort = START_PORT;

        // int port = findFreePort(START_PORT); // 5555 ve sonrası için boş olan ilk
        // portu verir

        // YENİ: Program argümanlarından leader mı follower mı belirle
        boolean isLeader = false;
        int port;

        // 1) Önce 5555'te leader olmayı dene
        try {
            ServerSocket test = new ServerSocket(START_PORT); // 5555 portunu açmayı dene bakma amaçlı boşsa lider
                                                              // kullancak
            test.close();

            // Buraya girdiysek 5555 BOŞ → lideriz
            isLeader = true;
            port = START_PORT;

            System.out.println("This node became LEADER on port 5555");

        } catch (IOException e) {
            // 5555 dolu → leader var → follower olacağız
            isLeader = false;
            port = -1;

            System.out.println("Leader already exists, this node is FOLLOWER");
        }

        if (!isLeader) {
            port = requestPortFromLeader(leaderHost, leaderPort, host);
            System.out.println("Follower received port from leader: " + port);
        }

        NodeInfo self = NodeInfo.newBuilder() // Üyenin kendisi
                .setHost(host)
                .setPort(port)
                .build();

        NodeRegistry registry = new NodeRegistry();
        FamilyServiceImpl service = new FamilyServiceImpl(registry, self);

        StorageServiceImpl storageService = new StorageServiceImpl();

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

        if (port != START_PORT) {
            notifyReadyToLeader(leaderHost, leaderPort, host, port);
        }

        discoverFamilyFromLeader(leaderHost, leaderPort, registry);
        startFamilyPrinter(registry, self);
        startHealthChecker(registry, self);

        server.awaitTermination();

    }

    private static void startLeaderTextListener(NodeRegistry registry, NodeInfo self) {
        // Sadece lider (5555 portlu node) bu methodu çağırmalı
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(6666)) {
                System.out.printf("Leader listening for text on TCP %s:%d%n",
                        self.getHost(), 6666);

                while (true) {
                    Socket client = serverSocket.accept();
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
        System.out.println("New TCP client connected: " + client.getRemoteSocketAddress());
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(client.getInputStream()))) {

            String line;
            while ((line = reader.readLine()) != null) {
                String text = line.trim();
                if (text.isEmpty()) {
                    continue;
                }

                // Kendi üstüne de yaz
                System.out.println(" Received from TCP: " + text);

                if (text.equalsIgnoreCase("STATS")) {
                    String statsReport = calculateLoadStats(registry);

                    // Sonucu client'a yaz
                    PrintWriter writer = new PrintWriter(client.getOutputStream(), true);
                    writer.println(statsReport);

                    // Döngünün başına dön (Broadcast yapmaya gerek yok)
                    continue;
                }

                try {
                    // 1) Komutu parse et
                    Command cmd = CommandParser.parse(text);

                    String result;

                    if (cmd instanceof SetCommand setCmd) {
                        int messageId = setCmd.getKey();
                        String messageText = setCmd.getValue();

                        // Disk'e yaz
                        writeMessageToDisk(messageId, messageText);

                        // Distributed replication
                        result = replicateToMembers(registry, self, messageId, messageText);

                    } else if (cmd instanceof GetCommand getCmd) {
                        int messageId = getCmd.getKey();

                        // Diskten oku

                        // String value = readMessageFromDisk(messageId);

                        // if (value == null) {
                        // Kendi diskinde yoksa, üyelerden almayı dene
                        // value = retrieveFromMembers(messageId);
                        // }

                        String value = retrieveFromMembers(messageId);

                        if (value == null) {
                            result = "NOT_FOUND";
                        } else {
                            result = "OK " + value;
                        }

                    } else {
                        result = "ERROR: Unknown command";
                    }
                    // Client'a cevabı yolluyoruz
                    PrintWriter writer = new PrintWriter(client.getOutputStream(), true);
                    writer.println(result);

                    long ts = System.currentTimeMillis();
                    ChatMessage msg = ChatMessage.newBuilder()
                            .setText(text)
                            .setFromHost(self.getHost())
                            .setFromPort(self.getPort())
                            .setTimestamp(ts)
                            .build();

                    // Tüm family üyelerine broadcast et
                    broadcastToFamily(registry, self, msg);

                } catch (IllegalArgumentException e) {
                    // Hatalı komut → ERROR dön
                    System.out.println("ERROR: " + e.getMessage());
                }

            }

        } catch (IOException e) {
            System.err.println("TCP client handler error: " + e.getMessage());
        } finally {
            try {
                client.close();
            } catch (IOException ignored) {
            }
        }
    }

    private static void broadcastToFamily(NodeRegistry registry,
            NodeInfo self,
            ChatMessage msg) {

        List<NodeInfo> members = registry.snapshot();

        for (NodeInfo n : members) {
            // Kendimize tekrar gönderme
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

                stub.receiveChat(msg);

                System.out.printf("Broadcasted message to %s:%d%n", n.getHost(), n.getPort());

            } catch (Exception e) {
                System.err.printf("Failed to send to %s:%d (%s)%n",
                        n.getHost(), n.getPort(), e.getMessage());
            } finally {
                if (channel != null) {
                    channel.shutdownNow();
                }
            }
        }
    }

    // private static int findFreePort(int startPort) {
    // int port = startPort;
    // while (true) {
    // try (ServerSocket ignored = new ServerSocket(port)) {
    // return port;
    // } catch (IOException e) {
    // port++;
    // }
    // }
    // }

    private static void discoverFamilyFromLeader(String leaderHost, int leaderPort,
            NodeRegistry registry) {

        ManagedChannel channel = null;
        try {
            channel = ManagedChannelBuilder
                    .forAddress(leaderHost, leaderPort)
                    .usePlaintext()
                    .build();

            FamilyServiceGrpc.FamilyServiceBlockingStub stub = FamilyServiceGrpc.newBlockingStub(channel);

            // join değil! sadece family snapshot al
            FamilyView view = stub.getFamily(Empty.newBuilder().build());

            // registry'ye "upsert" mantığıyla basmak en sağlıklısı:
            for (NodeInfo n : view.getMembersList()) {
                registry.upsert(n);
            }

            System.out.println("Family pulled from leader. size=" + registry.snapshot().size());

        } finally {
            if (channel != null)
                channel.shutdownNow();
        }
    }

    private static void startFamilyPrinter(NodeRegistry registry, NodeInfo self) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        scheduler.scheduleAtFixedRate(() -> {
            List<NodeInfo> members = registry.snapshot();
            System.out.println("======================================");
            System.out.printf("Family at %s:%d (me)%n", self.getHost(), self.getPort());
            System.out.println("Time: " + LocalDateTime.now());
            System.out.println("Members:");

            for (NodeInfo n : members) {
                boolean isMe = n.getHost().equals(self.getHost()) && n.getPort() == self.getPort();
                System.out.printf(" - %s:%d%s%n",
                        n.getHost(),
                        n.getPort(),
                        isMe ? " (me)" : "");
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

    private static final File MESSAGE_DIR = new File("messages");

    static {
        if (!MESSAGE_DIR.exists()) {
            MESSAGE_DIR.mkdirs();
        }
    }

    private static void writeMessageToDisk(int id, String msg) { // String id -> int id
        File file = new File(MESSAGE_DIR, id + ".msg");
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
            bw.write(msg);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String readMessageFromDisk(int id) { // String id -> int id
        File file = new File(MESSAGE_DIR, id + ".msg");
        if (!file.exists()) {
            return null;
        }

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            return br.readLine();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private static String replicateToMembers(NodeRegistry registry, NodeInfo self, int messageId, String messageText) {
        int tolerance = ToleranceConfig.getTolerance();
        List<NodeInfo> allMembers = registry.snapshot();

        List<NodeInfo> eligibleMembers = new ArrayList<>();
        for (NodeInfo member : allMembers) {
            if (!(member.getHost().equals(self.getHost()) && member.getPort() == self.getPort())) {
                eligibleMembers.add(member);
            }
        }

        if (eligibleMembers.isEmpty()) {
            System.out.println("No members available for replication, only leader exists");
            return "OK (ONLY LEADER)";
        }

        Collections.sort(eligibleMembers, Comparator.comparingInt(NodeInfo::getPort));
        int replicasNeeded = Math.min(tolerance, eligibleMembers.size());
        int startIndex = messageId % eligibleMembers.size();
        List<NodeInfo> selectedMembers = new ArrayList<>();
        for (int i = 0; i < replicasNeeded; i++) {
            // Döngüsel seçim (Wrap around): Listenin sonuna gelince başa dön
            int currentIndex = (startIndex + i) % eligibleMembers.size();
            selectedMembers.add(eligibleMembers.get(currentIndex));
        }

        // 4. ADIM: Seçilen üyelere gönder
        int successCount = 0;
        for (NodeInfo member : selectedMembers) {
            ManagedChannel channel = null;
            try {
                channel = ManagedChannelBuilder
                        .forAddress(member.getHost(), member.getPort())
                        .usePlaintext()
                        .build();

                StorageServiceGrpc.StorageServiceBlockingStub stub = StorageServiceGrpc.newBlockingStub(channel);

                StoredMessage msg = StoredMessage.newBuilder()
                        .setId(messageId)
                        .setText(messageText)
                        .build();

                StoreResult result = stub.store(msg);

                if (result.getSuccess()) {
                    REPLICA_TRACKER.addReplica(messageId, member);
                    successCount++;
                    System.out.printf("Replicated msg %d to %s:%d (LoadBalanced)%n",
                            messageId, member.getHost(), member.getPort());
                }

            } catch (Exception e) {
                System.err.printf("Failed to replicate to %s:%d - %s%n",
                        member.getHost(), member.getPort(), e.getMessage());
            } finally {
                if (channel != null) {
                    channel.shutdownNow();
                }
            }
        }

        if (successCount >= 1) { // En az 1 yere bile gitse OK sayabiliriz (tasarım tercihi)
            return "OK";
        } else {
            return "ERROR: Replication failed";
        }
    }

    private static String retrieveFromMembers(int messageId) {
        List<NodeInfo> members = REPLICA_TRACKER.getMembersForMessage(messageId);

        if (members.isEmpty()) {
            System.out.println("No replica information found for message " + messageId);
            return null;
        }

        for (NodeInfo member : members) {
            ManagedChannel channel = null;
            try {
                channel = ManagedChannelBuilder
                        .forAddress(member.getHost(), member.getPort())
                        .usePlaintext()
                        .build();

                StorageServiceGrpc.StorageServiceBlockingStub stub = StorageServiceGrpc.newBlockingStub(channel);

                MessageId msgId = MessageId.newBuilder()
                        .setId(messageId)
                        .build();

                StoredMessage response = stub.retrieve(msgId);

                if (response != null && !response.getText().isEmpty()) {
                    System.out.printf("Retrieved message %d from %s:%d%n",
                            messageId, member.getHost(), member.getPort());
                    return response.getText();
                }

            } catch (Exception e) {
                System.err.printf("Failed to retrieve from %s:%d - %s%n",
                        member.getHost(), member.getPort(), e.getMessage());
            } finally {
                if (channel != null) {
                    channel.shutdownNow();
                }
            }
        }

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
        sb.append("============================\n");

        // =====================================================
        // 5) OLUŞAN RAPORU STRING OLARAK DÖN
        // =====================================================

        return sb.toString();
    }

    // ============================================================
    // Leader'dan "boş port" istemek için helper metot
    // ============================================================
    // Bu metot FOLLOWER (lider olmayan node) tarafından çağrılır.
    //
    // Mantık:
    // 1) Leader'a gRPC ile bağlanır (leaderHost:leaderPort).
    // 2) join RPC'sini "port=0" ile çağırır.
    // - port=0 demek: "Ben daha port seçmedim, bana bir port ATA" isteği.
    // 3) Leader FamilyView döner ve içine assignedPort koyar.
    // 4) Biz de o assignedPort'u alıp geri döndürürüz.
    //
    // ÖNEMLİ:
    // - Bu metot "port taraması" yapmaz.
    // - Sadece leader'ın kendi tuttuğu state'e göre önerdiği portu alır.
    // - Port gerçekten boş mu dolu mu, follower node gRPC server'ı başlatırken
    // anlaşılır.
    // ============================================================
    private static int requestPortFromLeader(String leaderHost, int leaderPort, String myHost) {

        // ------------------------------------------------------------
        // 1) Leader'a gRPC channel aç
        // ------------------------------------------------------------
        // ManagedChannel = gRPC'nin TCP bağlantı nesnesi gibi düşünebilirsin.
        // Biz leader'a bağlanmak için bunu kullanıyoruz.
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(leaderHost, leaderPort) // Leader'ın host:port'u (örn 127.0.0.1:5555)
                .usePlaintext() // TLS yok -> local test için düz bağlantı
                .build();

        // ------------------------------------------------------------
        // 2) Leader'ın FamilyService'ine çağrı yapacak stub oluştur
        // ------------------------------------------------------------
        // BlockingStub = çağrı bitene kadar buradaki thread bekler.
        // join() çağrısı bitince sonuç döner.
        FamilyServiceGrpc.FamilyServiceBlockingStub stub = FamilyServiceGrpc.newBlockingStub(channel);

        // ------------------------------------------------------------
        // 3) join isteği için NodeInfo request hazırla
        // ------------------------------------------------------------
        // Kritik kural:
        // request.port == 0 => "Ben port istemeye geldim"
        //
        // myHost:
        // Bu follower node'un kendi host'u (örn 127.0.0.1)
        //
        // ready=false:
        // Çünkü henüz gRPC server'ı o portta AÇMADIK.
        // Yani "hazırım" değiliz. Sadece port istiyoruz.
        NodeInfo request = NodeInfo.newBuilder()
                .setHost(myHost) // Benim hostum
                .setPort(0) // PORT İSTE modunu tetikler (leader bunu görünce allocatePort yapar)
                .setReady(false) // Henüz server açılmadı -> ready değil
                .build();

        // ------------------------------------------------------------
        // 4) Leader'a join çağrısını gönder
        // ------------------------------------------------------------
        // Leader tarafında FamilyServiceImpl.join(request) çalışır.
        // Eğer request.port == 0 ise:
        // - leader allocatePort(host) ile bir port seçer
        // - FamilyView içine assignedPort koyar
        // - registry'ye pending (ready=false) olarak ekler
        FamilyView view = stub.join(request);

        // ------------------------------------------------------------
        // 5) Channel'ı kapat
        // ------------------------------------------------------------
        // Açılan bağlantıyı mutlaka kapatıyoruz yoksa connection leak olur.
        channel.shutdownNow();

        // ------------------------------------------------------------
        // 6) Leader'ın verdiği portu dön
        // ------------------------------------------------------------
        // view.getAssignedPort():
        // Leader'ın "sana önerdiğim port" dediği sayı.
        // Bu portu follower node alıp kendi gRPC server'ını o portta başlatacak.
        return view.getAssignedPort();
    }

    // ============================================================
    // Leader'a "ben server'ı açtım, artık hazırım" bildirir
    // ============================================================
    private static void notifyReadyToLeader(String leaderHost, int leaderPort, String myHost, int myPort) {

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(leaderHost, leaderPort)
                .usePlaintext()
                .build();

        try {
            FamilyServiceGrpc.FamilyServiceBlockingStub stub = FamilyServiceGrpc.newBlockingStub(channel);

            // port != 0 ve ready=true -> Leader bunu "ready bildirimi" kabul eder
            NodeInfo readyReq = NodeInfo.newBuilder()
                    .setHost(myHost)
                    .setPort(myPort)
                    .setReady(true)
                    .build();

            // Leader join() içinde upsert yapacak
            stub.join(readyReq);

        } finally {
            channel.shutdownNow();
        }
    }

}
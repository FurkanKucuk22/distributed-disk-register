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

                        // Disk'e yaz (Disabled)
                        // writeMessageToDisk(messageId, messageText);

                        // Distributed replication (Consistent Hashing)
                        result = replicateToMembers(registry, self, messageId, messageText);

                    } else if (cmd instanceof GetCommand getCmd) {
                        int messageId = getCmd.getKey();

                        // Diskten oku (Disabled)
                        // String value = readMessageFromDisk(messageId);

                        // Kendi diskinde yoksa, üyelerden almayı dene
                        String value = retrieveFromMembers(registry, messageId);

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

        // Consistent Hashing ile seçim
        // 50 virtual node kullanarak router oluştur
        ConsistentHashRouter router = new ConsistentHashRouter(eligibleMembers, 50);

        List<NodeInfo> selectedMembers = router.getPreferenceList(String.valueOf(messageId), tolerance);

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

    private static String retrieveFromMembers(NodeRegistry registry, int messageId) {
        // 1) Önce yerel tracker'a bak (biz yazmışsak orada kayıtlıdır)
        List<NodeInfo> members = REPLICA_TRACKER.getMembersForMessage(messageId);

        // 2) Eğer yerel tracker'da yoksa (başka node yazmış olabilir),
        // Consistent Hashing ile "olması gereken" yerleri hesapla.
        if (members == null || members.isEmpty()) {
            System.out.println("Local tracker empty, calculating location with Consistent Hashing...");

            List<NodeInfo> allNodes = registry.snapshot();

            if (allNodes.isEmpty())
                return null;

            ConsistentHashRouter router = new ConsistentHashRouter(allNodes, 50);
            int tolerance = ToleranceConfig.getTolerance();

            // Verinin olması gereken nodeları bul
            members = router.getPreferenceList(String.valueOf(messageId), tolerance);
        }

        if (members == null || members.isEmpty()) {
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
        Map<Integer, List<NodeInfo>> data = REPLICA_TRACKER.getSnapshot();

        // Her node'un kaç mesaj tuttuğunu saymak için map
        Map<String, Integer> nodeCounts = new HashMap<>();

        // 1) MESAJ → NODE DAĞILIMINI MEVCUT TABLOYU OKU VE SAY
        for (List<NodeInfo> nodes : data.values()) {
            for (NodeInfo node : nodes) {
                String key = node.getHost() + ":" + node.getPort();
                nodeCounts.put(key, nodeCounts.getOrDefault(key, 0) + 1);
            }
        }

        // 2) RAPOR METNİNİ OLUŞTUR
        StringBuilder sb = new StringBuilder();

        // Başlık
        sb.append("\n=== MESAJ DAĞILIMLARI ===\n");

        // Sistemde toplam kaç farklı mesaj var
        sb.append("Toplam Kaydedilen Mesajlar: ")
                .append(data.size())
                .append("\n");

        // 3) TÜM AİLE ÜYELERİNİ LİSTELE
        List<NodeInfo> allMembers = new ArrayList<>(registry.snapshot());
        allMembers.sort(Comparator.comparingInt(NodeInfo::getPort));

        // 4) HER NODE İÇİN KAÇ MESAJ VAR YAZ
        for (NodeInfo member : allMembers) {
            String key = member.getHost() + ":" + member.getPort();
            int count = nodeCounts.getOrDefault(key, 0);

            sb.append(String.format(
                    "Node %s -> %d mesaj\n",
                    key,
                    count));
        }
        sb.append("============================\n");

        return sb.toString();
    }

    private static int requestPortFromLeader(String leaderHost, int leaderPort, String myHost) {

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(leaderHost, leaderPort)
                .usePlaintext()
                .build();

        FamilyServiceGrpc.FamilyServiceBlockingStub stub = FamilyServiceGrpc.newBlockingStub(channel);

        NodeInfo request = NodeInfo.newBuilder()
                .setHost(myHost)
                .setPort(0)
                .setReady(false)
                .build();

        FamilyView view = stub.join(request);

        channel.shutdownNow();

        return view.getAssignedPort();
    }

    private static void notifyReadyToLeader(String leaderHost, int leaderPort, String myHost, int myPort) {

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(leaderHost, leaderPort)
                .usePlaintext()
                .build();

        try {
            FamilyServiceGrpc.FamilyServiceBlockingStub stub = FamilyServiceGrpc.newBlockingStub(channel);

            NodeInfo readyReq = NodeInfo.newBuilder()
                    .setHost(myHost)
                    .setPort(myPort)
                    .setReady(true)
                    .build();

            stub.join(readyReq);

        } finally {
            channel.shutdownNow();
        }
    }

}

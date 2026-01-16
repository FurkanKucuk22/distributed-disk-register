package com.example.family;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap; // DEĞİŞİKLİK: gRPC channel cache için eklendi
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.example.family.SetGetCommand.Command;
import com.example.family.SetGetCommand.CommandParser;
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
    private static final String LEADER_HOST = "192.168.1.172";
    private static final int LEADER_PORT = 5555;

    // STORE Removed
    private static final MessageReplicaTracker REPLICA_TRACKER = new MessageReplicaTracker();

    // =========================================================
    // DEĞİŞİKLİK: Performans bayrakları (benchmark için)
    // =========================================================
    private static final boolean BENCHMARK_MODE = true; // true: broadcast kapalı + log azaltılır
    private static final boolean VERBOSE_LOGS = false; // true yaparsan eski gibi çok log basar

    // =========================================================
    // DEĞİŞİKLİK: gRPC channel cache (en büyük hız kazanımı)
    // =========================================================
    private static final Map<String, ManagedChannel> CHANNEL_CACHE = new ConcurrentHashMap<>();

    private static ManagedChannel getChannel(String host, int port) {
        String key = host + ":" + port;
        return CHANNEL_CACHE.computeIfAbsent(key,
                k -> ManagedChannelBuilder.forAddress(host, port).usePlaintext().build());
    }

    // =========================================================
    // DEĞİŞİKLİK: Router cache (her SET/GET'te ring kurma yok)
    // =========================================================
    private static volatile ConsistentHashRouter ROUTER_CACHE = null;
    private static volatile String ROUTER_KEY = ""; // üyeler değişince yenilenecek

    private static ConsistentHashRouter getOrBuildRouter(List<NodeInfo> eligibleMembers) {
        // members listesi için basit bir "imza" üret (host:port sıralı)
        eligibleMembers.sort(Comparator.comparing(NodeInfo::getHost).thenComparingInt(NodeInfo::getPort));
        StringBuilder sb = new StringBuilder();
        for (NodeInfo n : eligibleMembers)
            sb.append(n.getHost()).append(":").append(n.getPort()).append("|");
        String key = sb.toString();

        ConsistentHashRouter cached = ROUTER_CACHE;
        if (cached != null && key.equals(ROUTER_KEY)) {
            return cached;
        }

        // DEĞİŞİKLİK: router sadece üyeler değişince yeniden kuruluyor
        ConsistentHashRouter fresh = new ConsistentHashRouter(eligibleMembers, 50);
        ROUTER_CACHE = fresh;
        ROUTER_KEY = key;
        return fresh;
    }

    public static void main(String[] args) throws Exception {
        ToleranceConfig.loadConfig();

        String host = getMyLanIp();
        System.out.println("MY HOST = " + host);
        System.out.println("LEADER  = " + LEADER_HOST + ":" + LEADER_PORT);

        int port;

        if (host.equals(LEADER_HOST) && canBindPort(LEADER_PORT)) {
            port = LEADER_PORT;
            System.out.println("LEADER (bound 5555)");
        } else {
            // follower: port iste + yerelde doluysa tekrar iste
            while (true) {
                port = requestPortFromLeader(LEADER_HOST, LEADER_PORT, host);
                if (canBindPort(port))
                    break;
                System.out.println("Port " + port + " is busy locally, asking again...");
            }
            System.out.println("FOLLOWER PORT = " + port);
        }

        NodeInfo self = NodeInfo.newBuilder()
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

        // DEĞİŞİKLİK: Kapanırken channel cache’i düzgün kapat
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (ManagedChannel ch : CHANNEL_CACHE.values()) {
                try {
                    ch.shutdownNow();
                } catch (Exception ignored) {
                }
            }
        }, "ShutdownHook-Channels"));

        if (port == START_PORT) {
            startLeaderTextListener(registry, self);
        } else {
            notifyReadyToLeader(LEADER_HOST, LEADER_PORT, host, port);
        }

        discoverFamilyFromLeader(LEADER_HOST, LEADER_PORT, registry);
        startFamilyRefresher(registry, self);
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

                    // DEĞİŞİKLİK: TCP tarafında Nagle kapat (server side)
                    try {
                        client.setTcpNoDelay(true);
                    } catch (Exception ignored) {
                    }

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

        if (VERBOSE_LOGS) {
            System.out.println("New TCP client connected: " + client.getRemoteSocketAddress());
        }

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(client.getInputStream()))) {

            String line;
            while ((line = reader.readLine()) != null) {
                String text = line.trim();
                if (text.isEmpty())
                    continue;

                // DEĞİŞİKLİK: Benchmark'ta bu log çok pahalı -> kapattık
                if (VERBOSE_LOGS)
                    System.out.println(" Received from TCP: " + text);

                if (text.equalsIgnoreCase("STATS")) {
                    String statsReport = calculateLoadStats(registry);
                    PrintWriter writer = new PrintWriter(client.getOutputStream(), true);
                    writer.println(statsReport);
                    continue; // broadcast yok
                }

                try {
                    Command cmd = CommandParser.parse(text);

                    String result;

                    if (cmd instanceof SetCommand setCmd) {
                        int messageId = setCmd.getKey();
                        String messageText = setCmd.getValue();

                        // Distributed replication (Consistent Hashing)
                        result = replicateToMembers(registry, self, messageId, messageText);

                    } else if (cmd instanceof GetCommand getCmd) {
                        int messageId = getCmd.getKey();
                        String value = retrieveFromMembers(registry, messageId);

                        if (value == null)
                            result = "NOT_FOUND";
                        else
                            result = "OK " + value;

                    } else {
                        result = "ERROR: Unknown command";
                    }

                    PrintWriter writer = new PrintWriter(client.getOutputStream(), true);
                    writer.println(result);

                    // =========================================================
                    // DEĞİŞİKLİK: Benchmark modunda broadcast kapalı (aşırı RPC)
                    // =========================================================
                    if (!BENCHMARK_MODE) {
                        long ts = System.currentTimeMillis();
                        ChatMessage msg = ChatMessage.newBuilder()
                                .setText(text)
                                .setFromHost(self.getHost())
                                .setFromPort(self.getPort())
                                .setTimestamp(ts)
                                .build();

                        broadcastToFamily(registry, self, msg);
                    }

                } catch (IllegalArgumentException e) {
                    if (VERBOSE_LOGS)
                        System.out.println("ERROR: " + e.getMessage());
                    // protokol gereği client'a error dönmek istiyorsan:
                    PrintWriter writer = new PrintWriter(client.getOutputStream(), true);
                    writer.println("ERROR: " + e.getMessage());
                }
            }

        } catch (IOException e) {
            if (VERBOSE_LOGS)
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
            if (n.getHost().equals(self.getHost()) && n.getPort() == self.getPort())
                continue;

            try {
                // DEĞİŞİKLİK: channel aç-kapat yok, cache kullanılıyor
                ManagedChannel channel = getChannel(n.getHost(), n.getPort());
                FamilyServiceGrpc.FamilyServiceBlockingStub stub = FamilyServiceGrpc.newBlockingStub(channel);
                stub.receiveChat(msg);

                if (VERBOSE_LOGS) {
                    System.out.printf("Broadcasted message to %s:%d%n", n.getHost(), n.getPort());
                }

            } catch (Exception e) {
                if (VERBOSE_LOGS) {
                    System.err.printf("Failed to send to %s:%d (%s)%n", n.getHost(), n.getPort(), e.getMessage());
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

            FamilyView view = stub.getFamily(Empty.newBuilder().build());
            for (NodeInfo n : view.getMembersList())
                registry.upsert(n);

            // System.out.println("Family pulled from leader. size=" + registry.snapshot().size());

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
            System.out.printf("Node Status [%s:%d]%n", self.getHost(), self.getPort());
            System.out.println("Time: " + LocalDateTime.now());

            System.out.println("Family Members:");
            for (NodeInfo n : members) {
                boolean isMe = n.getHost().equals(self.getHost()) && n.getPort() == self.getPort();
                System.out.printf(" - %s:%d%s%n", n.getHost(), n.getPort(), isMe ? " (me)" : "");
            }

            if (self.getPort() == START_PORT) {
                System.out.println("\n--- LEADER REPORT ---");
                System.out.print(calculateLoadStats(registry));
            }

        }, 3, PRINT_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    private static void startHealthChecker(NodeRegistry registry, NodeInfo self) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        scheduler.scheduleAtFixedRate(() -> {
            List<NodeInfo> members = registry.snapshot();

            for (NodeInfo n : members) {
                if (n.getHost().equals(self.getHost()) && n.getPort() == self.getPort())
                    continue;

                try {
                    // DEĞİŞİKLİK: channel aç-kapat yok, cache kullanılıyor
                    ManagedChannel channel = getChannel(n.getHost(), n.getPort());
                    FamilyServiceGrpc.FamilyServiceBlockingStub stub = FamilyServiceGrpc.newBlockingStub(channel);
                    stub.getFamily(Empty.newBuilder().build());

                } catch (Exception e) {
                    System.out.printf("Node %s:%d unreachable, removing from family%n", n.getHost(), n.getPort());
                    registry.remove(n);

                    // DEĞİŞİKLİK: ölen node'un channel'ını cache'ten sök (temizlik)
                    String key = n.getHost() + ":" + n.getPort();
                    ManagedChannel ch = CHANNEL_CACHE.remove(key);
                    if (ch != null) {
                        try {
                            ch.shutdownNow();
                        } catch (Exception ignored) {
                        }
                    }
                }
            }

        }, 5, 10, TimeUnit.SECONDS);
    }

    private static final File MESSAGE_DIR = new File("messages");
    static {
        if (!MESSAGE_DIR.exists())
            MESSAGE_DIR.mkdirs();
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
            if (VERBOSE_LOGS)
                System.out.println("No members available for replication, only leader exists");
            return "OK (ONLY LEADER)";
        }

        // DEĞİŞİKLİK: router cache kullan (her requestte yeniden kurma yok)
        ConsistentHashRouter router = getOrBuildRouter(eligibleMembers);
        List<NodeInfo> selectedMembers = router.getPreferenceList(String.valueOf(messageId), tolerance);

        int successCount = 0;
        for (NodeInfo member : selectedMembers) {
            try {
                // DEĞİŞİKLİK: channel cache (build/shutdown yok)
                ManagedChannel channel = getChannel(member.getHost(), member.getPort());
                StorageServiceGrpc.StorageServiceBlockingStub stub = StorageServiceGrpc.newBlockingStub(channel);

                StoredMessage msg = StoredMessage.newBuilder()
                        .setId(messageId)
                        .setText(messageText)
                        .build();

                StoreResult result = stub.store(msg);

                if (result.getSuccess()) {
                    REPLICA_TRACKER.addReplica(messageId, member);
                    successCount++;

                    // DEĞİŞİKLİK: Benchmark'ta bu log çok pahalı -> kapatıldı
                    if (VERBOSE_LOGS) {
                        System.out.printf("Replicated msg %d to %s:%d (LoadBalanced)%n",
                                messageId, member.getHost(), member.getPort());
                    }
                }

            } catch (Exception e) {
                if (VERBOSE_LOGS) {
                    System.err.printf("Failed to replicate to %s:%d - %s%n",
                            member.getHost(), member.getPort(), e.getMessage());
                }
            }
        }

        return (successCount >= 1) ? "OK" : "ERROR: Replication failed";
    }

    private static String retrieveFromMembers(NodeRegistry registry, int messageId) {
        List<NodeInfo> members = REPLICA_TRACKER.getMembersForMessage(messageId);

        if (members == null || members.isEmpty()) {
            // DEĞİŞİKLİK: Benchmark'ta bu log da pahalı -> kapatıldı
            if (VERBOSE_LOGS)
                System.out.println("Local tracker empty, calculating location with Consistent Hashing...");

            List<NodeInfo> allNodes = registry.snapshot();
            if (allNodes.isEmpty())
                return null;

            // NOT: GET tarafında self dahil tüm nodelar olabilir
            ConsistentHashRouter router = new ConsistentHashRouter(allNodes, 50);
            int tolerance = ToleranceConfig.getTolerance();
            members = router.getPreferenceList(String.valueOf(messageId), tolerance);
        }

        if (members == null || members.isEmpty())
            return null;

        for (NodeInfo member : members) {
            try {
                // DEĞİŞİKLİK: channel cache (build/shutdown yok)
                ManagedChannel channel = getChannel(member.getHost(), member.getPort());
                StorageServiceGrpc.StorageServiceBlockingStub stub = StorageServiceGrpc.newBlockingStub(channel);

                MessageId msgId = MessageId.newBuilder().setId(messageId).build();
                StoredMessage response = stub.retrieve(msgId);

                if (response != null && !response.getText().isEmpty()) {
                    if (VERBOSE_LOGS) {
                        System.out.printf("Retrieved message %d from %s:%d%n",
                                messageId, member.getHost(), member.getPort());
                    }
                    return response.getText();
                }

            } catch (Exception e) {
                if (VERBOSE_LOGS) {
                    System.err.printf("Failed to retrieve from %s:%d - %s%n",
                            member.getHost(), member.getPort(), e.getMessage());
                }
            }
        }

        return null;
    }

    private static String calculateLoadStats(NodeRegistry registry) {
        Map<Integer, List<NodeInfo>> data = REPLICA_TRACKER.getSnapshot();
        Map<String, Integer> nodeCounts = new HashMap<>();

        for (List<NodeInfo> nodes : data.values()) {
            for (NodeInfo node : nodes) {
                String key = node.getHost() + ":" + node.getPort();
                nodeCounts.put(key, nodeCounts.getOrDefault(key, 0) + 1);
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("\n=== MESAJ DAĞILIMLARI ===\n");
        sb.append("Toplam Kaydedilen Mesajlar: ").append(data.size()).append("\n");

        List<NodeInfo> allMembers = new ArrayList<>(registry.snapshot());
        allMembers.sort(Comparator.comparingInt(NodeInfo::getPort));

        for (NodeInfo member : allMembers) {
            String key = member.getHost() + ":" + member.getPort();
            int count = nodeCounts.getOrDefault(key, 0);
            sb.append(String.format("Node %s -> %d mesaj\n", key, count));
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

    private static boolean canBindPort(int port) {
        try (ServerSocket ss = new ServerSocket(port)) {
            ss.setReuseAddress(true);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private static String getMyLanIp() {
        try {
            Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
            while (ifaces.hasMoreElements()) {
                NetworkInterface ni = ifaces.nextElement();
                if (!ni.isUp() || ni.isLoopback())
                    continue;

                Enumeration<InetAddress> addrs = ni.getInetAddresses();
                while (addrs.hasMoreElements()) {
                    InetAddress a = addrs.nextElement();
                    if (a instanceof Inet4Address && a.isSiteLocalAddress()) {
                        String ip = a.getHostAddress();
                        if (!ip.startsWith("169.254.")) {
                            return ip;
                        }
                    }
                }
            }
        } catch (Exception ignored) {
        }
        return "127.0.0.1";
    }

    // ✅ EKLENDİ: Üyeler leader'dan family listesini periyodik çeker (registry
    // güncellenir)
    private static void startFamilyRefresher(NodeRegistry registry, NodeInfo self) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        scheduler.scheduleAtFixedRate(() -> {
            try {
                // Leader da çekebilir, zararı yok; ama asıl amaç followerların güncellenmesi
                discoverFamilyFromLeader(LEADER_HOST, LEADER_PORT, registry);

                // Kendini kaybetme ihtimaline karşı garanti:
                registry.upsert(self);

            } catch (Exception e) {
                System.err.println("[REFRESH] Family refresh failed: " + e.getMessage());
            }
        }, 5, 2, TimeUnit.SECONDS); // 2 sn’de bir güncelle (istersen 5 yaparız)
    }

}

package com.example.family;

import family.Empty;
import family.FamilyServiceGrpc;
import family.FamilyView;
import family.NodeInfo;

import java.util.HashMap;
import java.util.Map;

import family.ChatMessage;
import io.grpc.stub.StreamObserver;

// ============================================================
// Bu sınıf artık "Leader/Registry" gibi davranacak.
// JOIN çağrısı 2 farklı amaçla kullanılacak:
//
// 1) Port isteme:
//    request.port == 0  --> "Ben geldim, bana port ata"
//
// 2) Ready bildirimi:
//    request.port != 0 && request.ready == true  --> "Ben o portta server açtım, artık hazırım"
//
// NOT: Burada "ağ taraması" yok.
// Leader sadece kendi tuttuğu state'e (kayıtlara) göre port öneriyor.
// Node o portu bind etmeyi dener, olmazsa tekrar ister (NodeMain tarafında).
// ============================================================

public class FamilyServiceImpl extends FamilyServiceGrpc.FamilyServiceImplBase {

    private final NodeRegistry registry; // Bu node’un bildiği family üyeleri listesi (kimler var).
    private final NodeInfo self; // Bu node’un kendi bilgisi.

    // nextPortByHost:
    // - Her host için (aynı makine/IP) bir sonraki önerilecek portu tutar.
    // - Örn: "127.0.0.1" için son verilen port 5558 ise,
    // bir sonraki 5559'dan devam eder.
    private final Map<String, Integer> nextPortByHost = new HashMap<>();

    // membersByKey:
    // - Leader'ın "kayıt defteri"
    // - Key: "host:port" (örn "127.0.0.1:5556")
    // - Value: NodeInfo (ready true/false dahil)
    //
    // Neden var?
    // - Port verirken "bu host:port daha önce aileye verilmiş mi?" kontrolünü
    // buradan yapıyoruz.
    // - Bu kontrol "network taraması" değil, "leader'ın bildiği state" kontrolü.
    private final Map<String, NodeInfo> membersByKey = new HashMap<>();

    // BASE_PORT:
    // - Leader üyeler için port önerirken buradan başlar.
    // - Örn leader 5555 ise, üyeler 5556+ olsun diye ayarladık.
    private static final int BASE_PORT = 5556;

    public FamilyServiceImpl(NodeRegistry registry, NodeInfo self) {
        this.registry = registry;
        this.self = self;
        this.registry.add(self); // node kendini kendi listesine ekliyor.

        // ============================================================
        // YENİ: Leader kendini membersByKey defterine yazar
        // ============================================================
        // Bu sayede leader'ın state'i "tam" olur.
        // Port atarken "host:port daha önce verilmiş mi?" kontrolü bu map üzerinden
        // yapılıyor.
        // Eğer leader kendini kaydetmezse state eksik kalır.
        membersByKey.put(key(self), self);
    }

    // ============================================================
    // YENİ: NodeInfo -> "host:port" string anahtar
    // ============================================================
    private String key(NodeInfo n) {
        return n.getHost() + ":" + n.getPort();
    }

    // ============================================================
    // YENİ: Port tahsis fonksiyonu (leader logic)
    // ============================================================
    // Leader kendi kayıtlarına bakarak (membersByKey) bir port önerir.

    private final java.util.Set<Integer> allocatedPorts = new java.util.HashSet<>();
    private int nextPort = 5556;

    private synchronized int allocatePort() {
        int port = nextPort;

        while (membersByKey.containsKey("ANY:" + port)) { // aşağıda anlatıcam
            port++;
        }

        nextPort = port + 1;
        return port;
    }

    // ============================================================
    // JOIN = Port iste veya Ready bildir
    // ============================================================
    @Override
    public synchronized void join(NodeInfo request, StreamObserver<FamilyView> responseObserver) {

        System.out.println("[JOIN] host=" + request.getHost()
                + " port=" + request.getPort()
                + " ready=" + request.getReady());

        // assignedPort:
        // - Sadece request.port == 0 durumunda (port isteği) dolu olur.
        // - Diğer durumlarda 0 kalır.
        int assignedPort = 0;

        // ------------------------------------------------------------
        // 1) PORT İSTEĞİ:
        // request.port == 0 ise node şöyle diyor:
        // "Ben geldim ama portum yok, bana port ata."
        // ------------------------------------------------------------
        if (request.getPort() == 0) {
            assignedPort = allocatePort();

            // SADECE rezerv defterine yaz (membersByKey), registry'ye ekleme!
            NodeInfo pending = NodeInfo.newBuilder()
                    .setHost(request.getHost())
                    .setPort(assignedPort)
                    .setReady(false)
                    .build();

            membersByKey.put(key(pending), pending);
        }

        // ------------------------------------------------------------
        // 2) READY BİLDİRİMİ:
        // request.port != 0 ve request.ready == true ise node şunu diyor:
        // "Ben bu portta server açtım, artık aktifim."
        // ------------------------------------------------------------
        else if (request.getReady()) {

            // readyNode: aynısı ama ready=true
            NodeInfo readyNode = NodeInfo.newBuilder()
                    .setHost(request.getHost())
                    .setPort(request.getPort())
                    .setReady(true)
                    .build();

            // Leader defterini güncelle: pending -> ready
            membersByKey.put(key(readyNode), readyNode);

            // Registry tarafında da güncelleme yapılmalı.
            // Çünkü registry listesinde eski pending (ready=false) duruyor olabilir.
            // upsert = varsa sil, güncelini ekle.
            registry.upsert(readyNode); // ⚠️ NodeRegistry'ye upsert eklemelisin
        }

        // ------------------------------------------------------------
        // 3) Cevap: FamilyView dön
        // - members: tüm aile üyeleri
        // - assignedPort: sadece port isteğinde dolu
        // ------------------------------------------------------------
        FamilyView view = FamilyView.newBuilder()
                .addAllMembers(registry.snapshot())
                .setAssignedPort(assignedPort)
                .build();

        responseObserver.onNext(view);
        responseObserver.onCompleted();
    }

    @Override
    public void getFamily(Empty request, StreamObserver<FamilyView> responseObserver) {
        FamilyView view = FamilyView.newBuilder()
                .addAllMembers(registry.snapshot()) // registry’de kim varsa ekle
                .build();

        responseObserver.onNext(view);
        responseObserver.onCompleted();
    }

    // Diğer düğümlerden broadcast mesajı geldiğinde
    @Override
    public void receiveChat(ChatMessage request, StreamObserver<Empty> responseObserver) {
        System.out.println("💬 Incoming message:");
        System.out.println("  From: " + request.getFromHost() + ":" + request.getFromPort());
        System.out.println("  Text: " + request.getText());
        System.out.println("  Timestamp: " + request.getTimestamp());
        System.out.println("--------------------------------------");

        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }
}

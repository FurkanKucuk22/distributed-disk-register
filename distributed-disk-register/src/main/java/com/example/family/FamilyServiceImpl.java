package com.example.family;

import family.Empty;
import family.FamilyServiceGrpc;
import family.FamilyView;
import family.NodeInfo;
<<<<<<< HEAD
import family.ChatMessage;
import io.grpc.stub.StreamObserver;

public class FamilyServiceImpl extends FamilyServiceGrpc.FamilyServiceImplBase {

    private final NodeRegistry registry;
    private final NodeInfo self;
=======

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
>>>>>>> main

    public FamilyServiceImpl(NodeRegistry registry, NodeInfo self) {
        this.registry = registry;
        this.self = self;
<<<<<<< HEAD
        this.registry.add(self);
    }

    @Override
    public void join(NodeInfo request, StreamObserver<FamilyView> responseObserver) {
        registry.add(request);

        FamilyView view = FamilyView.newBuilder()
                .addAllMembers(registry.snapshot())
=======
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
    private synchronized int allocatePort(String host) {

        // Bu host için bir sonraki önerilecek portu al
        // Yoksa BASE_PORT'tan başla
        int port = nextPortByHost.getOrDefault(host, BASE_PORT);

        // Bu host:port daha önce ailede kullanılmış mı?
        // Kullanılmışsa port++ yapıp devam et.
        // Bu "ağda dolu mu?" değil; "leader daha önce bu portu dağıtmış mı?" kontrolü.
        while (membersByKey.containsKey(host + ":" + port)) {
            port++;
        }

        // Bu host için bir sonraki denenecek portu güncelle (port+1)
        nextPortByHost.put(host, port + 1);

        // Seçilen portu geri döndür
        return port;
    }

    // ============================================================
    //  JOIN = Port iste veya Ready bildir
    // ============================================================
    @Override
    public synchronized void join(NodeInfo request, StreamObserver<FamilyView> responseObserver) {

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

            // Leader bu host için bir port önerir (state'e göre)
            assignedPort = allocatePort(request.getHost());

            // Node henüz o portu bind edip açmadı, o yüzden ready=false
            NodeInfo pending = NodeInfo.newBuilder()
                    .setHost(request.getHost())
                    .setPort(assignedPort)
                    .setReady(false) // Bu port dağıtıldı ama node henüz açmadı rezerve edildi 
                    .build();

            // Leader defterine yaz
            membersByKey.put(key(pending), pending);

            // Registry listesine de ekle (aile snapshot'ında görünsün)
            registry.add(pending);
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
>>>>>>> main
                .build();

        responseObserver.onNext(view);
        responseObserver.onCompleted();
    }

<<<<<<< HEAD
    @Override
    public void getFamily(Empty request, StreamObserver<FamilyView> responseObserver) {
        FamilyView view = FamilyView.newBuilder()
                .addAllMembers(registry.snapshot())
=======
   
    @Override
    public void getFamily(Empty request, StreamObserver<FamilyView> responseObserver) {
        FamilyView view = FamilyView.newBuilder()
                .addAllMembers(registry.snapshot()) // registry’de kim varsa ekle
>>>>>>> main
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

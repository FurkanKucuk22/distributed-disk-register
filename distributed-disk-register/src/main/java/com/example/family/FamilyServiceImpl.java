package com.example.family;

import family.Empty;
import family.FamilyServiceGrpc;
import family.FamilyView;
import family.NodeInfo;
import family.ChatMessage;
import io.grpc.stub.StreamObserver;

public class FamilyServiceImpl extends FamilyServiceGrpc.FamilyServiceImplBase {


    private final NodeRegistry registry; // Bu node’un bildiği family üyeleri listesi (kimler var).
    private final NodeInfo self; // Bu node’un kendi bilgisi.

    public FamilyServiceImpl(NodeRegistry registry, NodeInfo self) {
        this.registry = registry;
        this.self = self;
        this.registry.add(self); // node kendini kendi listesine ekliyor.
    }

    @Override
    public void join(NodeInfo request, StreamObserver<FamilyView> responseObserver) {
        registry.add(request);

        FamilyView view = FamilyView.newBuilder()
                .addAllMembers(registry.snapshot())
                .build();

        responseObserver.onNext(view); // Join isteğine karşılık FamilyView’ı client’a yolla.
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

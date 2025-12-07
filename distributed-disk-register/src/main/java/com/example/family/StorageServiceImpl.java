package com.example.family; // Paket adın neyse ona dikkat et

import com.example.family.SetGetCommand.*;


import family.MessageId;
import family.StorageServiceGrpc;
import family.StoredMessage;
import family.StoreResult;
import io.grpc.stub.StreamObserver;

public class StorageServiceImpl extends StorageServiceGrpc.StorageServiceImplBase {

  private final DataStore dataStore;

  // Servisimiz çalışmak için bir Veri Deposuna ihtiyaç duyar
  public StorageServiceImpl(DataStore dataStore) {
    this.dataStore = dataStore;
  }

  @Override
  public void store(StoredMessage request, StreamObserver<StoreResult> responseObserver) {
    // 1. Gelen Protobuf mesajından verileri al
    // Not: SetCommand constructor'ı String beklediği için şimdilik çeviriyoruz.
    // İleride SetCommand'a doğrudan "request" nesnesini veren bir constructor
    // ekleyerek bunu hızlandırabiliriz.
    String key = String.valueOf(request.getId());
    String value = request.getText();

    // 2. Komutu oluştur ve çalıştır
    SetCommand command = new SetCommand(key, value);
    command.execute(dataStore);

    // 3. Sonucu hazırla (Başarılı)
    StoreResult result = StoreResult.newBuilder().setSuccess(true).build();

    // 4. Cevabı gönder ve işlemi kapat
    responseObserver.onNext(result);
    responseObserver.onCompleted();

    System.out.println("GRPC ile veri kaydedildi: " + key + " -> " + value);
  }

  @Override
  public void retrieve(MessageId request, StreamObserver<StoredMessage> responseObserver) {
    // 1. İstenen ID'yi al
    String key = String.valueOf(request.getId());

    // 2. Komutu oluştur ve çalıştır
    GetCommand command = new GetCommand(key);
    String foundValue = command.execute(dataStore);

    // 3. Bulunan değeri Protobuf mesajına paketle
    // Eğer "NOT_FOUND" döndüyse boş veya hata mesajı dönebiliriz, şimdilik olduğu
    // gibi dönelim.
    StoredMessage response = StoredMessage.newBuilder()
        .setId(request.getId())
        .setText(foundValue)
        .build();

    // 4. Cevabı gönder
    responseObserver.onNext(response);
    responseObserver.onCompleted();

    System.out.println("GRPC ile veri okundu: " + key);
  }
}
package com.example.family;

import com.example.family.SetGetCommand.*;

import family.MessageId;
import family.StorageServiceGrpc;
import family.StoredMessage;
import family.StoreResult;
import io.grpc.stub.StreamObserver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class StorageServiceImpl extends StorageServiceGrpc.StorageServiceImplBase {

  private final DataStore dataStore;
  // ARTIK STATIC DEĞİL: Her nesnenin (node'un) kendi klasör yolu var
  private final File messageDir;

  // Servisimiz çalışmak için bir Veri Deposuna ihtiyaç duyar
  // Constructor değişti: Artık port numarasını da alıyor
  public StorageServiceImpl(DataStore dataStore, int port) {
    this.dataStore = dataStore;

    // 1. ADIM: Ana "messages" klasörünü tanımla ve yoksa oluştur
    File parentDir = new File("messages");
    if (!parentDir.exists()) {
      parentDir.mkdirs();
    }

    // 2. ADIM: Ana klasörün içine node'a özel klasörü (messages_5555) oluştur
    this.messageDir = new File(parentDir, "messages_" + port);

    if (!this.messageDir.exists()) {
      this.messageDir.mkdirs();
      System.out.println("Klasör yolu oluşturuldu: " + this.messageDir.getPath());
    }
    // Düğüm açıldığında diskteki eski mesajları hafızaya (DataStore) yükle
    loadExistingMessages();
  }

  // Gelen id ve text bilgilerini alır, hem memory'e hem de disk'e kaydeder
  @Override
  public void store(StoredMessage request, StreamObserver<StoreResult> responseObserver) {
    try {
      // 1. Gelen Protobuf mesajından verileri al
      int id = request.getId();
      String value = request.getText();

      // 2. Memory'e kaydet
      dataStore.set(id, value);

      // 3. Disk'e kaydet
      writeMessageToDisk(id, value);

      // 4. Sonucu hazırla (Başarılı)
      StoreResult result = StoreResult.newBuilder().setSuccess(true).build();

      // 5. Cevabı gönder ve işlemi kapat
      responseObserver.onNext(result);
      responseObserver.onCompleted();

      System.out.println("GRPC ile veri kaydedildi (disk+memory): " + id + " -> " + value);
    } catch (Exception e) {
      System.err.println("Store operation failed: " + e.getMessage());
      StoreResult result = StoreResult.newBuilder().setSuccess(false).build();
      responseObserver.onNext(result);
      responseObserver.onCompleted();
    }
  }

  // Gelen id bilgisine göre diskteki mesajı okur ve geri döner
  @Override
  public void retrieve(MessageId request, StreamObserver<StoredMessage> responseObserver) {
    // 1. İstenen ID'yi al
    int id = request.getId();

    // 2) Önce CACHE’ten dene (RAM)
    String foundValue = dataStore.get(id);

    // 3) Cache’te yoksa DISK’ten oku
    if (foundValue == null) {
      foundValue = readMessageFromDisk(id);

      // Diskte bulunduysa cache’e koy (sonraki okumalar hızlı)
      if (foundValue != null) {
        dataStore.set(id, foundValue);
      }
    }

    // 4. Bulunan değeri Protobuf mesajına paketle
    if (foundValue == null) {
      foundValue = "NOT_FOUND";
    }

    StoredMessage response = StoredMessage.newBuilder()
        .setId(request.getId())
        .setText(foundValue)
        .build();

    // 4. Cevabı gönder
    responseObserver.onNext(response);
    responseObserver.onCompleted();

    System.out.println("GRPC ile veri okundu: " + id + " -> " + foundValue);
  }

  // ==========================================
  // DOSYA İŞLEMLERİ (PERSISTENCE)
  // ==========================================

  // Verilen id ve mesajı diske yazar
  private void writeMessageToDisk(int id, String msg) {
    File file = new File(this.messageDir

        , id + ".msg");
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
      bw.write(msg);
    } catch (IOException e) {
      System.err.println("Failed to write message to disk: " + e.getMessage());
    }
  }

  // Verilen id'ye sahip mesajı diskten okur
  private String readMessageFromDisk(int id) {
    File file = new File(this.messageDir

        , id + ".msg");
    if (!file.exists()) {
      return null;
    }

    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      return br.readLine();
    } catch (IOException e) {
      System.err.println("Failed to read message from disk: " + e.getMessage());
      return null;
    }
  }

  // Düğüm açıldığında diskteki tüm mesajları hafızaya (DataStore) yükler
  private void loadExistingMessages() {
    File[] files = this.messageDir.listFiles((dir, name) -> name.endsWith(".msg"));

    if (files != null) {
      for (File file : files) {
        try {
          // Dosya adından ID'yi al (Örn: "5.msg" -> 5)
          String fileName = file.getName();
          int id = Integer.parseInt(fileName.replace(".msg", ""));

          // Dosya içeriğini oku
          String value = readMessageFromDisk(id);

          // Hafızaya yükle
          if (value != null) {
            dataStore.set(id, value);
          }
        } catch (Exception e) {
          System.err.println("Eski mesaj yüklenirken hata: " + file.getName());
        }
      }
      System.out.println("Diskten " + files.length + " mesaj geri yüklendi.");
    }
  }
}
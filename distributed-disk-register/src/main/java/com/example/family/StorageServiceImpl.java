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

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

public class StorageServiceImpl extends StorageServiceGrpc.StorageServiceImplBase {

  private static final File MESSAGE_DIR = new File("messages");

  static {
    if (!MESSAGE_DIR.exists()) {
      MESSAGE_DIR.mkdirs();
    }
  }

  // Servisimiz (Artık sadece disk kullanıyor)
  public StorageServiceImpl() {
  }

  @Override
  public void store(StoredMessage request, StreamObserver<StoreResult> responseObserver) {
    try {
      // 1. Gelen Protobuf mesajından verileri al
      int id = request.getId();
      String value = request.getText();

      // 2. Memory'e kaydetme kısmını kaldırdık (Gereksiz RAM kullanımı)
      // dataStore.set(id, value);

      // 3. Disk'e kaydet
      writeMessageToDisk(id, value);

      // 4. Sonucu hazırla (Başarılı)
      StoreResult result = StoreResult.newBuilder().setSuccess(true).build();

      // 5. Cevabı gönder ve işlemi kapat
      responseObserver.onNext(result);
      responseObserver.onCompleted();

      System.out.println("GRPC ile veri kaydedildi (disk): " + id + " -> " + value);
    } catch (Exception e) {
      System.err.println("Store operation failed: " + e.getMessage());
      StoreResult result = StoreResult.newBuilder().setSuccess(false).build();
      responseObserver.onNext(result);
      responseObserver.onCompleted();
    }
  }

  @Override
  public void retrieve(MessageId request, StreamObserver<StoredMessage> responseObserver) {
    // 1. İstenen ID'yi al
    int id = request.getId();

    // 2. Diskten oku
    String foundValue = readMessageFromDisk(id);

    // 3. Bulunan değeri Protobuf mesajına paketle
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

  // ZERO-COPY (Memory Mapped File) Yöntemi
private void writeMessageToDisk(int id, String msg) {
    File file = new File(MESSAGE_DIR, id + ".msg");
    
    // 1. String veriyi byte dizisine çevir (Maliyetli ama zorunlu adım)
    byte[] data = msg.getBytes(StandardCharsets.UTF_8);

    // 2. RandomAccessFile ve FileChannel aç
    // "rw" modu: Hem okuma hem yazma izni verir (Map işlemi için gereklidir)
    try (RandomAccessFile raf = new RandomAccessFile(file, "rw");
         FileChannel channel = raf.getChannel()) {

        // 3. Dosyayı hafızaya haritala (Mapping)
        // FileChannel.MapMode.READ_WRITE: Hem okuyup hem yazacağız
        // 0: Başlangıç pozisyonu
        // data.length: Dosyanın boyutu (byte dizisi kadar yer açar)
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, data.length);

        // 4. Veriyi direkt olarak haritalanmış hafıza alanına koy
        // Bu işlem işletim sistemi seviyesinde diske asenkron olarak yansıtılır.
        buffer.put(data);

        // İsteğe bağlı: buffer.force(); 
        // Verinin fiziksel diske yazıldığını garanti etmek için kullanılabilir 

    } catch (IOException e) {
        System.err.println("Zero-Copy yazma hatası: " + e.getMessage());
    }
}

  private String readMessageFromDisk(int id) {
    File file = new File(MESSAGE_DIR, id + ".msg");
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
}
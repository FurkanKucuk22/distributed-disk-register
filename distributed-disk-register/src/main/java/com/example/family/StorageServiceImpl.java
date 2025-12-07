package com.example.family;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import com.example.family.SetGetCommand.DataStore;

import family.MessageId;
import family.StorageServiceGrpc;
import family.StoreResult;
import family.StoredMessage;
import io.grpc.stub.StreamObserver;

public class StorageServiceImpl extends StorageServiceGrpc.StorageServiceImplBase {

    private final DataStore store;

    // Her node'un kendi mesaj klasörü
    private static final File MESSAGE_DIR = new File("messages");

    static {
        if (!MESSAGE_DIR.exists()) {
            MESSAGE_DIR.mkdirs();
        }
    }

    public StorageServiceImpl(DataStore store) {
        this.store = store; //  FINAL alan burada initialize edildi
    }

    @Override
    public void store(StoredMessage request, StreamObserver<StoreResult> responseObserver ) {

        int id = request.getId();
        String text = request.getText();
        String key = String.valueOf(id);

        // 1) RAM'deki DataStore'a yaz
        store.set(key, text);

        // 2) Disk'e yaz
        writeMessageToDisk(key, text);

        // 3) Sonuç dön
        StoreResult result = StoreResult.newBuilder()
                .setSuccess(true)
                .build();

        responseObserver.onNext(result);
        responseObserver.onCompleted();
    }

    @Override
    public void retrieve(MessageId request, StreamObserver<StoredMessage> responseObserver) {

        int id = request.getId();
        String key = String.valueOf(id);

        // 1) Önce DataStore'dan dene 
        // Bunu yapma sebebi RAM'den hızlı erişim sağlamak
        String value = store.get(key);

        // 2) RAM'de yoksa diskten dene
        if (value == null || "NOT_FOUND".equals(value)) {
            value = readMessageFromDisk(key);
        }

        // Eğer diskte de yoksa boş string ata
        if (value == null) {
            value = "";  // NOT_FOUND case'i üst katmanda handle edebilirsin
        }

        StoredMessage msg = StoredMessage.newBuilder()
                .setId(id)
                .setText(value)
                .build();

        responseObserver.onNext(msg);
        responseObserver.onCompleted();
    }



    private static void writeMessageToDisk(String id, String msg) {
      File file = new File(MESSAGE_DIR, id + ".msg");
      try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
          bw.write(msg);
      } catch (IOException e) {
          e.printStackTrace();
      }
    }

    private static String readMessageFromDisk(String id) {
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
}



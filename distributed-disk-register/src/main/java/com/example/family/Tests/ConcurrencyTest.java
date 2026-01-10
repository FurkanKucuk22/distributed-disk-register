package com.example.family.Tests;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConcurrencyTest {

    private static final int CLIENT_COUNT = 3;       // 3 İstemci
    private static final int MSG_PER_CLIENT = 1000;  // Her biri 3000 mesaj (Top: 9000)
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6666;

    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== EŞ ZAMANLI YÜK TESTİ BAŞLIYOR ===");
        System.out.println("İstemci Sayısı: " + CLIENT_COUNT);
        System.out.println("Toplam Mesaj: " + (CLIENT_COUNT * MSG_PER_CLIENT));

        // İş parçacığı havuzu oluştur
        ExecutorService executor = Executors.newFixedThreadPool(CLIENT_COUNT);
        
        // Tüm threadlerin aynı anda başlaması için sayaç
        CountDownLatch latch = new CountDownLatch(CLIENT_COUNT);

        long start = System.currentTimeMillis();

        for (int i = 0; i < CLIENT_COUNT; i++) {
            final int clientId = i;
            executor.submit(() -> {
                try {
                    runClient(clientId);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown(); // İşini bitiren haber verir
                }
            });
        }

        // Herkesin bitmesini bekle
        latch.await();
        executor.shutdown();

        long end = System.currentTimeMillis();
        System.out.println("\n=== TEST TAMAMLANDI ===");
        System.out.println("Toplam Süre: " + (end - start) + " ms");
    }

    private static void runClient(int clientId) {
        try (Socket socket = new Socket(HOST, PORT);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            int startId = clientId * MSG_PER_CLIENT; // Örn: Client 0 -> 0-2999 arası
            int endId = startId + MSG_PER_CLIENT;

            System.out.println("Client-" + clientId + " bağlandı. ID Aralığı: " + startId + " - " + endId);

            for (int i = startId; i < endId; i++) {
                String cmd = "SET " + i + " Client_" + clientId + "_Verisi_" + i;
                out.println(cmd);
                
                String resp = in.readLine();
                if (resp == null || !resp.startsWith("OK")) {
                    System.err.println("HATA Client-" + clientId + " Msg-" + i + ": " + resp);
                }
            }
            System.out.println("Client-" + clientId + " BİTTİ.");

        } catch (Exception e) {
            System.err.println("Client-" + clientId + " çöktü: " + e.getMessage());
        }
    }
}
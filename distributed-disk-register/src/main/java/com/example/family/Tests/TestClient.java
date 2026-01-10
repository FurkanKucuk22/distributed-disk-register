package com.example.family.Tests;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class TestClient {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6666; // Liderin dinlediği port
    private static final int MESSAGE_COUNT = 1000; // Kaç mesaj atılacak?

    public static void main(String[] args) {
        System.out.println("Test Başlıyor: " + MESSAGE_COUNT + " adet mesaj gönderilecek...");

        try (Socket socket = new Socket(HOST, PORT);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            long startTime = System.currentTimeMillis();

            for (int i = 0; i < MESSAGE_COUNT; i++) {
                // Mesaj Formatı: SET <id> <text>
                // Örnek: SET 0 Mesaj_Icerigi_0
                String command = "SET " + i + " Mesaj_Icerigi_" + i;
                
                // Gönder
                out.println(command);

                // Cevabı Oku (Sunucunun OK veya ERROR demesini bekle)
                String response = in.readLine();
                
                if (response == null) {
                    System.err.println("Sunucu bağlantıyı kesti!");
                    break;
                }

                // Her 100 mesajda bir ekrana bilgi bas (Takip etmek için)
                if (i % 100 == 0) {
                    System.out.println("Gönderildi: " + i + " -> Cevap: " + response);
                }
            }

            long endTime = System.currentTimeMillis();
            System.out.println("Test Bitti!");
            System.out.println("Geçen Süre: " + (endTime - startTime) + " ms");

        } catch (IOException e) {
            System.err.println("Bağlantı hatası: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
package com.example.family.Tests;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class PerformanceTest {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6666; // Liderin portu
    private static final int DATA_SIZE = 1024 * 1024; // 1 MB (1.048.576 byte)

    public static void main(String[] args) {
        System.out.println("=== 1 MB PERFORMANS TESTİ BAŞLIYOR ===");
        
        // 1. ADIM: 1 MB'lık veriyi oluştur
        System.out.println("1 MB veri hazırlanıyor...");
        String largePayload = generateLargeString(DATA_SIZE);
        System.out.println("Veri hazır. Boyut: " + largePayload.length() + " karakter.");

        try (Socket socket = new Socket(HOST, PORT);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            // --- SET TESTİ ---
            System.out.println("\n--- SET TESTİ (Yazma) ---");
            long startSet = System.currentTimeMillis();
            
            // Komut: SET 999 <1MB_DATA>
            out.println("SET 999 " + largePayload);
            
            // Cevabı bekle (OK gelmeli)
            String setResponse = in.readLine();
            
            long endSet = System.currentTimeMillis();
            long durationSet = endSet - startSet;
            
            System.out.println("Sunucu Cevabı: " + setResponse);
            System.out.println("SET Süresi: " + durationSet + " ms");


            // --- GET TESTİ ---
            System.out.println("\n--- GET TESTİ (Okuma) ---");
            long startGet = System.currentTimeMillis();
            
            // Komut: GET 999
            out.println("GET 999");
            
            // Cevabı bekle (1MB verinin geri gelmesi lazım)
            String getResponse = in.readLine();
            
            long endGet = System.currentTimeMillis();
            long durationGet = endGet - startGet;

            if (getResponse != null) {
                System.out.println("Veri alındı. Gelen Boyut: " + getResponse.length() + " karakter");
                if (getResponse.length() == largePayload.length()) {
                    System.out.println("Doğrulama: BAŞARILI (Veri tam geldi)");
                } else {
                    System.err.println("Doğrulama: HATALI (Veri eksik geldi)");
                }
            } else {
                System.err.println("Hata: Sunucudan boş cevap döndü.");
            }
            
            System.out.println("GET Süresi: " + durationGet + " ms");

        } catch (IOException e) {
            System.err.println("Bağlantı hatası: " + e.getMessage());
        }
    }

    // Belirtilen boyutta 'a' harfinden oluşan string üretir
    private static String generateLargeString(int size) {
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            sb.append('a');
        }
        return sb.toString();
    }
}

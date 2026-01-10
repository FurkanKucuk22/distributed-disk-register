package com.example.family;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import family.NodeInfo;

public class MessageReplicaTracker {
    
    // Hafızadaki ana veri yapısı
    private final Map<Integer, List<NodeInfo>> messageToMembers = new ConcurrentHashMap<>();
    
    // Disk üzerindeki kayıt dosyası
    private static final File TRACKER_FILE = new File("distribution.log");

    // Constructor: Sınıf oluşurken eski verileri yükle
    public MessageReplicaTracker() {
        loadTrackerFromDisk();
    }

    public void addReplica(int messageId, NodeInfo member) {
        // 1. Önce RAM'e ekle (Program çalışırken hızlı erişim için)
        messageToMembers.computeIfAbsent(messageId, k -> new ArrayList<>()).add(member);
        
        // 2. Sonra Diske ekle (Kapanınca unutmamak için)
        appendTrackerToDisk(messageId, member);
    }

    public List<NodeInfo> getMembersForMessage(int messageId) {
        return messageToMembers.getOrDefault(messageId, new ArrayList<>());
    }

    public void removeDeadMember(NodeInfo deadMember) {
        // Ölen üye RAM'den silinir, böylece ona tekrar istek atılmaz.
        // Not: Log dosyasından silme işlemi yapmıyoruz (Performans için append-only tutuyoruz)
        for (List<NodeInfo> members : messageToMembers.values()) {
            members.removeIf(m -> m.getHost().equals(deadMember.getHost()) 
                              && m.getPort() == deadMember.getPort());
        }
    }

    public void printStats() {
        System.out.println("=== Message Replica Stats ===");
        System.out.println("Total messages tracked: " + messageToMembers.size());
        messageToMembers.forEach((id, members) -> {
            System.out.printf("Message %d -> %d replicas%n", id, members.size());
        });
    }

    public Map<Integer, List<NodeInfo>> getSnapshot() {
        return Collections.unmodifiableMap(messageToMembers);
    }

    // ==========================================
    //       DOSYA İŞLEMLERİ (PERSISTENCE)
    // ==========================================

    // Yeni bir dağıtım bilgisini dosyanın sonuna ekler
    private synchronized void appendTrackerToDisk(int messageId, NodeInfo member) {
        // Format: ID,HOST,PORT (Örn: 5,127.0.0.1,5556)
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(TRACKER_FILE, true))) {
            String line = messageId + "," + member.getHost() + "," + member.getPort();
            bw.write(line);
            bw.newLine();
        } catch (IOException e) {
            System.err.println("Lider: Tracker dosyasına yazılamadı: " + e.getMessage());
        }
    }

    // Uygulama açılışında dosyayı okuyup RAM'i doldurur
    private void loadTrackerFromDisk() {
        if (!TRACKER_FILE.exists()) {
            return; // Dosya yoksa yapacak bir şey yok (İlk açılış)
        }

        System.out.println("Lider: Geçmiş dağılım verisi yükleniyor...");
        int count = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(TRACKER_FILE))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                String[] parts = line.split(",");
                if (parts.length == 3) {
                    try {
                        int id = Integer.parseInt(parts[0]);
                        String host = parts[1];
                        int port = Integer.parseInt(parts[2]);

                        // NodeInfo nesnesini oluştur
                        NodeInfo info = NodeInfo.newBuilder()
                                .setHost(host)
                                .setPort(port)
                                .build();

                        // RAM'deki listeye ekle (Burada appendTrackerToDisk çağırmıyoruz, döngü olur!)
                        messageToMembers.computeIfAbsent(id, k -> new ArrayList<>()).add(info);
                        count++;
                    } catch (NumberFormatException e) {
                        System.err.println("Satır parse edilemedi: " + line);
                    }
                }
            }
            System.out.println("Lider: " + count + " adet kayıt başarıyla geri yüklendi.");
        } catch (Exception e) {
            System.err.println("Tracker dosyası okunurken hata: " + e.getMessage());
        }
    }
}
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
    
    // =====================================================
    // RAM'DEKİ ANA VERİ YAPISI
    // =====================================================
    // messageId -> bu mesajın tutulduğu node'lar
    // Thread-safe olsun diye ConcurrentHashMap kullanıyoruz
    private final Map<Integer, List<NodeInfo>> messageToMembers = new ConcurrentHashMap<>();
    
    // =====================================================
    // DISK ÜZERİNDEKİ KALICI KAYIT DOSYASI
    // =====================================================
    // Uygulama kapanıp açılsa bile geçmiş replikasyon bilgileri
    // kaybolmasın diye kullanılıyor
    private static final File TRACKER_FILE = new File("distribution.log");

    // =====================================================
    // CONSTRUCTOR
    // =====================================================
    // Nesne oluşturulurken diskten eski kayıtları RAM'e yükler
    public MessageReplicaTracker() {
        loadTrackerFromDisk();
    }

    // =====================================================
    // DIŞARIDAN ÇAĞRILAN ANA METOT
    // =====================================================
    // Bir mesajın belirli bir node'a başarıyla replike edildiğini
    // hem RAM'e hem diske kaydeder
    public void addReplica(int messageId, NodeInfo member) {
        // 1) Önce RAM'e ekle (duplicate kontrolü var)
        addReplicaToMemory(messageId, member);
        
        // 2) Sonra diske ekle (kalıcılık için)
        appendTrackerToDisk(messageId, member);
    }

    // =====================================================
    // SADECE RAM'E EKLEME (KONTROLLÜ)
    // =====================================================
    private void addReplicaToMemory(int messageId, NodeInfo member) {

        // Mesaj için liste yoksa oluştur
        List<NodeInfo> currentMembers =
                messageToMembers.computeIfAbsent(messageId, k -> new ArrayList<>());

        // Aynı node daha önce eklenmiş mi kontrol et
        boolean alreadyExists = currentMembers.stream()
                .anyMatch(m ->
                        m.getHost().equals(member.getHost()) &&
                        m.getPort() == member.getPort()
                );

        // Duplicate yoksa listeye ekle
        if (!alreadyExists) {
            currentMembers.add(member);
        }
    }

    // =====================================================
    // BİR MESAJIN BULUNDUĞU NODE'LARI GETİR
    // =====================================================
    public List<NodeInfo> getMembersForMessage(int messageId) {
        return messageToMembers.getOrDefault(messageId, new ArrayList<>());
    }

    // =====================================================
    // ÖLMÜŞ NODE'U TÜM MESAJLARDAN TEMİZLE
    // =====================================================
    public void removeDeadMember(NodeInfo deadMember) {

        // Diskten silme YOK (append-only tasarım)
        // Sadece RAM'den çıkarıyoruz
        for (List<NodeInfo> members : messageToMembers.values()) {
            members.removeIf(m ->
                    m.getHost().equals(deadMember.getHost()) &&
                    m.getPort() == deadMember.getPort()
            );
        }
    }

    // =====================================================
    // DEBUG / LOG AMAÇLI İSTATİSTİK
    // =====================================================
    public void printStats() {
        System.out.println("=== Message Replica Stats ===");
        System.out.println("Total messages tracked: " + messageToMembers.size());
        messageToMembers.forEach((id, members) -> {
            System.out.printf("Message %d -> %d replicas%n", id, members.size());
        });
    }

    // =====================================================
    // SALT DIŞARDAN OKUNUR SNAPSHOT
    // =====================================================
    // Map'in dışarıdan değiştirilmesini engeller
    public Map<Integer, List<NodeInfo>> getSnapshot() {
        return Collections.unmodifiableMap(messageToMembers);
    }

    // =====================================================
    // DISK'E EKLEME (APPEND-ONLY)
    // =====================================================
    private synchronized void appendTrackerToDisk(int messageId, NodeInfo member) {
        // Format: messageId,host,port
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(TRACKER_FILE, true))) {
            String line = messageId + "," + member.getHost() + "," + member.getPort();
            bw.write(line);
            bw.newLine();
        } catch (IOException e) {
            System.err.println("Lider: Tracker dosyasına yazılamadı: " + e.getMessage());
        }
    }

    // =====================================================
    // UYGULAMA BAŞLARKEN DISKTEN YÜKLE
    // =====================================================
    private void loadTrackerFromDisk() {

        // Dosya yoksa (ilk çalıştırma) çık
        if (!TRACKER_FILE.exists()) {
            return;
        }

        System.out.println("Lider: Geçmiş dağılım verisi yükleniyor...");
        int count = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(TRACKER_FILE))) {
            String line;

            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                String[] parts = line.split(",");

                // Beklenen format: 3 parça
                if (parts.length == 3) {
                    try {
                        int id = Integer.parseInt(parts[0]);
                        String host = parts[1];
                        int port = Integer.parseInt(parts[2]);

                        NodeInfo info = NodeInfo.newBuilder()
                                .setHost(host)
                                .setPort(port)
                                .build();

                        // RAM'e kontrollü ekleme
                        addReplicaToMemory(id, info);
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

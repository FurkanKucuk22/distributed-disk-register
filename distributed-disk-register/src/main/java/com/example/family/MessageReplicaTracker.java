package com.example.family;

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
    // CONSTRUCTOR
    // =====================================================
    // Eskiden burada diskten log okuyup RAM'e yüklüyorduk.
    // Artık KESİNLİKLE diskten yükleme istemiyoruz.
    // Bu yüzden constructor boş kalıyor.
    public MessageReplicaTracker() {
        // loadTrackerFromDisk();  // ❌ ARTIK YOK: diskten yükleme yapılmasın
    }

    // =====================================================
    // DIŞARIDAN ÇAĞRILAN ANA METOT
    // =====================================================
    // Eskiden:
    // 1) RAM'e yaz
    // 2) Diske append et
    //
    // Artık:
    // Sadece RAM'e yazacağız. Disk yok.
    public void addReplica(int messageId, NodeInfo member) {

        // 1) RAM'e ekle (duplicate kontrolü var)
        addReplicaToMemory(messageId, member);

        // 2) Disk'e yazma kısmı KALDIRILDI
        // appendTrackerToDisk(messageId, member); // ❌ artık yok
    }

    // =====================================================
    // SADECE RAM'E EKLEME (KONTROLLÜ)
    // =====================================================
    private void addReplicaToMemory(int messageId, NodeInfo member) {

        // Mesaj için liste yoksa oluştur
        // computeIfAbsent: yoksa yeni ArrayList oluşturup map'e koyar
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
        // Eğer yoksa boş liste döndür
        // (Yeni ArrayList dönüyoruz ki dışarıdan değiştirseler bile map bozulmasın)
        return new ArrayList<>(messageToMembers.getOrDefault(messageId, new ArrayList<>()));
    }

    // =====================================================
    // ÖLMÜŞ NODE'U TÜM MESAJLARDAN TEMİZLE
    // =====================================================
    public void removeDeadMember(NodeInfo deadMember) {

        // Disk zaten kullanılmıyor (append-only yok).
        // Sadece RAM'den çıkarıyoruz.
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
    // SADECE OKUNUR SNAPSHOT
    // =====================================================
    // Dışarıdan map'in tamamen değiştirilmesini engeller
    public Map<Integer, List<NodeInfo>> getSnapshot() {
        return Collections.unmodifiableMap(messageToMembers);
    }

    // =====================================================
    // ⚠️ ESKİ DISK FONKSİYONLARI KALDIRILDI
    // =====================================================
    // appendTrackerToDisk(...) ❌
    // loadTrackerFromDisk()   ❌
    //
    // Çünkü artık "distribution.log" kullanılmayacak,
    // uygulama kapanıp açılınca replikasyon bilgisi sıfırdan başlayacak.
}

package com.example.family;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import family.NodeInfo;

public class MessageReplicaTracker {

    private final Map<Integer, List<NodeInfo>> messageToMembers = new ConcurrentHashMap<>();

    public void addReplica(int messageId, NodeInfo member) {
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

    public List<NodeInfo> getMembersForMessage(int messageId) {
        return messageToMembers.getOrDefault(messageId, new ArrayList<>());
    }

    public void removeDeadMember(NodeInfo deadMember) {
        for (List<NodeInfo> members : messageToMembers.values()) {
            members.removeIf(m -> m.getHost().equals(deadMember.getHost()) 
                              && m.getPort() == deadMember.getPort());
        }
    }
    // =====================================================
    // CONSTRUCTOR
    // =====================================================
    // Eskiden burada diskten log okuyup RAM'e yüklüyorduk.
    // Artık KESİNLİKLE diskten yükleme istemiyoruz.
    // Bu yüzden constructor boş kalıyor.
    
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
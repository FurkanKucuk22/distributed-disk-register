package com.example.family;

import family.NodeInfo;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Halka (Ring) olarak TreeMap kullanan Tutarlı Hashleme (Consistent Hashing)
 * uygulaması.
 * Daha iyi dağılım sağlamak için sanal düğümleri (virtual nodes) destekler.
 */
public class ConsistentHashRouter {
    private final SortedMap<Integer, NodeInfo> ring = new TreeMap<>();
    private final int numberOfReplicas;

    /**
     * @param nodes            Halkaya eklenecek fiziksel düğümler koleksiyonu.
     * @param numberOfReplicas Fiziksel düğüm başına düşen sanal düğüm sayısı.
     */
    public ConsistentHashRouter(Collection<NodeInfo> nodes, int numberOfReplicas) {
        this.numberOfReplicas = numberOfReplicas;
        if (nodes != null) {
            for (NodeInfo node : nodes) {
                addNode(node);
            }
        }
    }

    public void addNode(NodeInfo node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            // Sanal düğüm anahtarı oluştur: "host:port:i"
            // Halkaya yerleştirmek için sağlam bir hash fonksiyonu kullan
            ring.put(hash(node.getHost() + ":" + node.getPort() + ":" + i), node);
        }
    }

    public void removeNode(NodeInfo node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            ring.remove(hash(node.getHost() + ":" + node.getPort() + ":" + i));
        }
    }

    /**
     * Verilen anahtar için sorumlu olan 'count' adet benzersiz düğümün listesini
     * döner.
     *
     * @param key   Nesne anahtarı (örneğin string olarak messageId)
     * @param count İstenen replika sayısı
     * @return NodeInfo Listesi
     */
    public List<NodeInfo> getPreferenceList(String key, int count) {
        List<NodeInfo> result = new ArrayList<>();
        if (ring.isEmpty()) {
            return result;
        }

        int hash = hash(key);

        // Halkanın mantıksal "kuyruğu"
        SortedMap<Integer, NodeInfo> tailMap = ring.tailMap(hash);

        // Dairesel halkayı simüle etmek için kuyruk (tail) + baş (head) birleştirilir
        // İdeal olan, kuyruktan başlayıp başa sararak anahtarlar üzerinde yineleme
        // yapmaktır.

        List<Integer> circleKeys = new ArrayList<>(tailMap.keySet());
        if (circleKeys.isEmpty()) {
            // Eğer hash son düğümün ötesindeyse, başa sar
            circleKeys.addAll(ring.keySet());
        } else {
            // Daireyi tamamlamak için halkanın geri kalanını ekle
            // (Hash değerinden kesinlikle küçük olan tüm anahtarlar)
            circleKeys.addAll(ring.headMap(hash).keySet());
        }

        for (Integer nodeHash : circleKeys) {
            NodeInfo node = ring.get(nodeHash);

            // host:port bilgisine göre benzersizlik kontrolü yap

            boolean alreadySelected = false;
            for (NodeInfo n : result) {
                if (n.getHost().equals(node.getHost()) && n.getPort() == node.getPort()) {
                    alreadySelected = true;
                    break;
                }
            }

            if (!alreadySelected) {
                result.add(node);
            }

            if (result.size() >= count) {
                break;
            }
        }

        return result;
    }

    private int hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(key.getBytes());
            // Tamsayı (integer) hash üretmek için ilk 4 baytı kullan
            // Bu, MD5'i 32-bit int uzayına yansıtmanın standart bir yoludur
            long h = 0;
            for (int i = 0; i < 4; i++) {
                h = (h << 8) | ((int) digest[i] & 0xFF);
            }
            return (int) h;
        } catch (NoSuchAlgorithmException e) {
            // Yedek plan (beklenmez ama)
            return key.hashCode();
        }
    }
}

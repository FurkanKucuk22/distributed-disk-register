package com.example.family;

import family.NodeInfo;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class NodeRegistry {

    // NodeInfo nesnelerinden oluşan set ConcurrentHashMap.newKeySet for thread safe (eş zamanlı) işlemler
    private final Set<NodeInfo> nodes = ConcurrentHashMap.newKeySet();

    // Yeni bir node’u family listesine ekler.
    public void add(NodeInfo node) {
        nodes.add(node);
    }

    // Birden fazla node’u family listesine ekler.
    public void addAll(Collection<NodeInfo> others) {
        nodes.addAll(others);
    }

    // Tüm node’ları döner.
    public List<NodeInfo> snapshot() {
        return List.copyOf(nodes);
    }

    // Bir node’u family listesinden çıkarır.
    public void remove(NodeInfo node) {
        nodes.remove(node);
    }
}

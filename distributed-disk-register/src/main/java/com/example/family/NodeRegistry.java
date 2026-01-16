package com.example.family;

import family.NodeInfo;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class NodeRegistry {

    // Thread-safe node set
    private final Set<NodeInfo> nodes = ConcurrentHashMap.newKeySet();

    // Yeni bir node ekle
    public void add(NodeInfo node) {
        nodes.add(node);
    }

    // Birden fazla node ekle
    public void addAll(Collection<NodeInfo> others) {
        nodes.addAll(others);
    }

    // Snapshot al (immutable liste)
    public List<NodeInfo> snapshot() {
        return List.copyOf(nodes);
    }

    // ❗ host + port'a göre sil
    public void remove(NodeInfo node) {
        nodes.removeIf(n ->
                n.getHost().equals(node.getHost()) &&
                n.getPort() == node.getPort()
        );
    }

    // UPDATE + INSERT
    public synchronized void upsert(NodeInfo node) {
        remove(node); // aynı host:port varsa sil
        add(node);    // güncel hali ekle
    }
}

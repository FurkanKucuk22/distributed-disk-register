package com.example.family;

import family.NodeInfo;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class NodeRegistry {

<<<<<<< HEAD
    private final Set<NodeInfo> nodes = ConcurrentHashMap.newKeySet();

=======
    // Thread-safe node set
    private final Set<NodeInfo> nodes = ConcurrentHashMap.newKeySet();

    // Yeni bir node ekle
>>>>>>> main
    public void add(NodeInfo node) {
        nodes.add(node);
    }

<<<<<<< HEAD
=======
    // Birden fazla node ekle
>>>>>>> main
    public void addAll(Collection<NodeInfo> others) {
        nodes.addAll(others);
    }

<<<<<<< HEAD
=======
    // Snapshot al (immutable liste)
>>>>>>> main
    public List<NodeInfo> snapshot() {
        return List.copyOf(nodes);
    }

<<<<<<< HEAD
    public void remove(NodeInfo node) {
        nodes.remove(node);
=======
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
>>>>>>> main
    }
}

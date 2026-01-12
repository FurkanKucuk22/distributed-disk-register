package com.example.family.SetGetCommand;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class DataStore {

    // Kaç kayıt RAM’de tutulacak?
    private static final int MAX_ENTRIES = 2000;

    // LRU Cache: accessOrder=true => en son kullanılan sona gider
    private final Map<Integer, String> cache = Collections.synchronizedMap(
        new LinkedHashMap<Integer, String>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, String> eldest) {
                return size() > MAX_ENTRIES; // sınırı aşınca en eskiyi sil
            }
        }
    );

    public String set(int key, String value) {
        cache.put(key, value); // artık sınırlı cache
        return "OK";
    }

    public String get(int key) {
        return cache.get(key); // cache’te yoksa null döner
    }

    public int getSize() {
        return cache.size();
    }
}

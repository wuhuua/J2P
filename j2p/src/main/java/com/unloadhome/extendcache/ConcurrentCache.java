package com.unloadhome.extendcache;

import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentCache<K, V> implements ExtendCache<K, V> {
    private final int size;

    private volatile Map<K, V> frontMap;

    private volatile Map<K, V> liveMap;

    ConcurrentCache(int size) {
        this.size = size;
        this.frontMap = new ConcurrentHashMap<K, V>(size);
        this.liveMap = new WeakHashMap<>(size);
    }

    @Override
    public void put(K key, V val) {
        if (this.frontMap.size() >= size) {
            this.liveMap.putAll(this.frontMap);
            this.frontMap.clear();
        }
        this.frontMap.put(key, val);
    }

    @Override
    public V get(K key) {
        V val = this.frontMap.get(key);
        if (Objects.isNull(val)) {
            val = this.liveMap.get(key);
            if (Objects.nonNull(val)) {
                this.frontMap.put(key, val);
            }
        }
        return val;
    }

}
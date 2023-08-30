package com.unloadhome.extendcache;

public interface ExtendCache<K, V> {
    public void put(K key,V val);
    public V get(K key);
}

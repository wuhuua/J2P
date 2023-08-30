package com.unloadhome.extendcache;

public class CacheFactory {
    public static <K,V> ExtendCache<K,V> getExtendCache(int size){
        return new ConcurrentCache<K,V>(size);
    }
}

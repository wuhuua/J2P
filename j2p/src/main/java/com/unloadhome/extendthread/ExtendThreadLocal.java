package com.unloadhome.extendthread;

import java.util.Iterator;
import java.util.Map;
import java.util.WeakHashMap;

public class ExtendThreadLocal<T> extends InheritableThreadLocal<T> {

    private static final InheritableThreadLocal<WeakHashMap<ExtendThreadLocal<Object>, ?>> holder
        = new InheritableThreadLocal<WeakHashMap<ExtendThreadLocal<Object>, ?>>() {
        protected WeakHashMap<ExtendThreadLocal<Object>, ?> initialValue() {
            return new WeakHashMap<>();
        }

        protected WeakHashMap<ExtendThreadLocal<Object>, ?> childValue(
            WeakHashMap<ExtendThreadLocal<Object>, ?> parentValue) {
            return new WeakHashMap<>(parentValue);
        }
    };

    private static final InheritableThreadLocal<Object> updateLock = new InheritableThreadLocal<Object>() {
        protected Object initialValue() {
            return new Object();
        }

        protected Object childValue(Object parentValue) {
            return parentValue;
        }
    };

    public final T get() {
        T val = super.get();
        addThisToHolder();
        return val;
    }
    

    public final void set(T val) {
        super.set(val);
        addThisToHolder();
    }

    protected T copyVal(T refVal) {
        return refVal;
    }

    @SuppressWarnings("unchecked")
    private void addThisToHolder() {
        if (!holder.get().containsKey(this)) {
            holder.get().put((ExtendThreadLocal<Object>) this, null);
        }
    }

    @Override
    public final void remove() {
        removeThisFromHolder();
        super.remove();
    }

    private void superRemove() {
        super.remove();
    }

    private void removeThisFromHolder() {
        synchronized (updateLock){
            holder.get().remove(this);
        }
    }

    private static class Values {
        final WeakHashMap<ExtendThreadLocal<Object>, Object> values;

        final Object updateLock;

        private Values(WeakHashMap<ExtendThreadLocal<Object>, Object> values,Object updateLock) {
            this.values = values;
            this.updateLock=updateLock;
        }
    }

    public static Object capture() {
        WeakHashMap<ExtendThreadLocal<Object>, Object> values = new WeakHashMap<>();
        for (ExtendThreadLocal<Object> threadLocal : holder.get().keySet()) {
            values.put(threadLocal,threadLocal.copyVal(threadLocal.get()));
        }
        return new Values(values,updateLock.get());
    }

    public static Object replay(Object captured) {
        final Values capturedValues = (Values) captured;
        return new Values(replayValues(capturedValues.values),capturedValues.updateLock);
    }

    private static WeakHashMap<ExtendThreadLocal<Object>, Object> replayValues(
        WeakHashMap<ExtendThreadLocal<Object>, Object> captured) {
        WeakHashMap<ExtendThreadLocal<Object>, Object> backup = new WeakHashMap<>();
        for (final Iterator<ExtendThreadLocal<Object>> iterator = holder.get().keySet().iterator(); iterator.hasNext(); ) {
            ExtendThreadLocal<Object> threadLocal = iterator.next();
            backup.put(threadLocal, threadLocal.get());
            if (!captured.containsKey(threadLocal)) {
                synchronized (updateLock){
                    iterator.remove();
                }
                threadLocal.superRemove();
            }
        }
        setValuesTo(captured);
        return backup;
    }

    public static void restore(Object backup) {
        final Values values = (Values) backup;
        restoreValues(values.values);
    }

    private static void restoreValues(WeakHashMap<ExtendThreadLocal<Object>, Object> backup) {
        for (final Iterator<ExtendThreadLocal<Object>> iterator = holder.get().keySet().iterator(); iterator.hasNext(); ) {
            ExtendThreadLocal<Object> threadLocal = iterator.next();
            if (!backup.containsKey(threadLocal)) {
                synchronized (updateLock){
                    iterator.remove();
                }
                threadLocal.superRemove();
            }
        }
        setValuesTo(backup);
    }

    private static void setValuesTo(WeakHashMap<ExtendThreadLocal<Object>, Object> values) {
        for (Map.Entry<ExtendThreadLocal<Object>, Object> entry : values.entrySet()) {
            final ExtendThreadLocal<Object> threadLocal = entry.getKey();
            threadLocal.set(entry.getValue());
        }
    }

}



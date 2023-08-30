package com.unloadhome.extendthread;

import static com.unloadhome.extendthread.ExtendThreadLocal.restore;
import static com.unloadhome.extendthread.ExtendThreadLocal.capture;
import static com.unloadhome.extendthread.ExtendThreadLocal.replay;

import java.util.concurrent.atomic.AtomicReference;

public class ExtendRunnable implements Runnable {
    private final AtomicReference<Object> capturedRef;
    private final Runnable runnable;

    public ExtendRunnable(Runnable runnable) {
        this.capturedRef = new AtomicReference<Object>(capture());
        this.runnable = runnable;
    }

    @Override
    public void run() {
        final Object captured = capturedRef.get();
        final Object backup = replay(captured);
        try {
            runnable.run();
        } finally {
            restore(backup);
        }
    }
}

package com.cchis.dctm.util.export;

import com.cchis.dctm.util.export.util.StopWatchEx;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Don't have to be thread-safe
 */
public class Counter {
    private AtomicInteger started;
    private AtomicInteger success;
    private AtomicInteger failed;
    private StopWatchEx stopWatch;


    public Counter(boolean startTiming) {
        if (startTiming) {
            this.stopWatch = StopWatchEx.createStarted();
        }
        initCount();
    }

    private void initCount() {
        this.success = new AtomicInteger(0);
        this.failed = new AtomicInteger(0);
        this.started = new AtomicInteger(0);
    }

    public synchronized void startTiming() {
        if (stopWatch == null) {
            stopWatch = StopWatchEx.createStarted();
        }
    }

    public int getSuccess() {
        return success.get();
    }

    public int getFailed() {
        return failed.get();
    }

    public int getStarted() {
        return started.get();
    }

    /**
     *
     * @return
     */
    public synchronized int getCount() {
        return success.get() + failed.get();
    }

    public synchronized int getActiveCount() {
        return started.get() - getCount();
    }

    public synchronized boolean isActive() {
        return getActiveCount() != 0;
    }

    public StopWatchEx getStopWatch() {
        return stopWatch;
    }

    public void incrementSuccess() {
        success.incrementAndGet();
    }

    public void incrementFailed() {
        failed.incrementAndGet();
    }

    public void incrementStarted() {
        started.incrementAndGet();
    }

    public void stop() {
        if (stopWatch != null) {
            stopWatch.stopSilently();
        }
    }
}

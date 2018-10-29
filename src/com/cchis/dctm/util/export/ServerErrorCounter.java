package com.cchis.dctm.util.export;

import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

import static com.cchis.dctm.util.export.util.ExportConstants.MSG_SERVER_ERROR_COUNTER;

public final class ServerErrorCounter {
    private static final Logger LOG = Logger.getLogger(ServerErrorCounter.class);
    private static final ServerErrorCounter INSTANCE = new ServerErrorCounter();
    private final AtomicInteger count;

    private ServerErrorCounter() {
        count = new AtomicInteger(0);
    }

    public static ServerErrorCounter getInstance() {
        return INSTANCE;
    }

    void incrErrorCount() {
        int newValue = count.incrementAndGet();
        LOG.trace(String.format(MSG_SERVER_ERROR_COUNTER, newValue));
    }

    int getErrorCount() {
        return count.get();
    }

    void resetCounter() {
        int newValue = 0;
        count.set(newValue);
        LOG.trace(String.format(MSG_SERVER_ERROR_COUNTER, newValue));
    }

    void setCounter(int newValue) {
        count.set(newValue);
    }

}

package com.cchis.dctm.util.export.util;

public class StopWatchEx extends org.apache.commons.lang.time.StopWatch {

    public static StopWatchEx createStarted() {
        StopWatchEx stopWatch = new StopWatchEx();
        stopWatch.start();
        return stopWatch;
    }

    public long getTimeAndRestart() {
        stop();
        long getTime = getTime();
        reset();
        start();
        return getTime;
    }

    public void stopSilently() {
        try {
            stop();
        } catch (Exception ignore) { }
    }

    public long stopAndGetTime() {
        stop();
        return getTime();
    }

}

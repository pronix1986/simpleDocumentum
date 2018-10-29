package com.cchis.dctm.util.export;

import com.cchis.dctm.util.export.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.cchis.dctm.util.export.util.ExportConstants.*;

public final class ExecutorsHandler {

    private ExecutorsHandler() { }

    private static ExecutorService mainExecutor = null;
    private static ExecutorService slowExecutor = null;
    private static ExecutorService cleanupExecutor = null;
    private static ExecutorService bookExecutor = null;

    private static final List<CompletableFuture<Void>> bookFutures = new ArrayList<>();
    private static final List<CompletableFuture<Void>> cleanupFutures = new ArrayList<>();

    private static final AtomicLong expectedTaskCount = new AtomicLong(0);

    private static final Lock NEXT_BOOK_LOCK = new ReentrantLock();
    private static final Condition PROCEED_NEXT_BOOK_CONDITION = NEXT_BOOK_LOCK.newCondition();
    private static final Condition TASK_COUNT_SET_CONDITION = NEXT_BOOK_LOCK.newCondition();

    private static final double LOAD_FACTOR = 0.4;

    public static ExecutorService getMainExecutor() {
        if (mainExecutor == null) {
            mainExecutor = Executors.newFixedThreadPool(EXECUTOR_COUNT);
        }
        return mainExecutor;
    }

    public static ExecutorService getSlowExecutor() {
        if (slowExecutor == null) {
            slowExecutor = Executors.newFixedThreadPool(SLOW_CONFIG_EXECUTOR_COUNT);
        }
        return slowExecutor;
    }

    public static ExecutorService getBookExecutor() {
        if (bookExecutor == null) {
            bookExecutor = Executors.newFixedThreadPool(BOOK_EXECUTOR_COUNT);
        }
        return bookExecutor;
    }

    public static ExecutorService getCleanupExecutor() {
        if (cleanupExecutor == null) {
            cleanupExecutor = Executors.newSingleThreadExecutor(r -> {    // should not be a daemon
                Thread t = new Thread(r, "WebcCleanupThread");
                t.setPriority(Thread.MIN_PRIORITY);
                return t;
            });
        }
        return cleanupExecutor;
    }

    public static void shutdownAllExecutors() {
        shutdownMainExecutor();
        Util.shutdownExecutorService(getBookExecutor());
    }

    public static void shutdownMainExecutor() {
        Util.shutdownExecutorService(getCleanupExecutor(), CLEANUP_SHUTDOWN_TIMEOUT);
        Util.shutdownExecutorService(getSlowExecutor());
        Util.shutdownExecutorService(getMainExecutor());
    }

    public static void shutdownExecutorsBeforeSlowProcessing() {
        Util.shutdownExecutorService(getMainExecutor());
        Util.shutdownExecutorService(getBookExecutor());
    }

    public static boolean isSlowExecutor(ExecutorService executor) {
        return executor != null && executor == getSlowExecutor();
    }

    public static void waitBooksForCompletion(boolean waitCleanup) {
        waitCompletionAndClear(bookFutures);
        if (waitCleanup) waitCompletionAndClear(cleanupFutures);
    }

    public static void runBookTask(Runnable r) {
        runTask(getBookExecutor(), bookFutures, r);
    }

    public static void runCleanupTask(Runnable r) {
        runTask(getCleanupExecutor(), cleanupFutures, r);
    }

    public static <T> void runTask(ExecutorService executor, List<CompletableFuture<T>> futures, Supplier<T> s) {
        futures.add(CompletableFuture.supplyAsync(s, executor));
    }

    public static <T> void runTask(ExecutorService executor, List<CompletableFuture<T>> futures, Supplier<T> s, Consumer<T> c) {
        CompletableFuture<T> future = CompletableFuture.supplyAsync(s, executor);
        futures.add(future);
        future.thenAccept(c);
    }

    public static void runTask(ExecutorService executor, List<CompletableFuture<Void>> futures, Runnable r) {
        futures.add(CompletableFuture.runAsync(r, executor));
    }

    public static <T> void waitCompletion(List<CompletableFuture<T>> futures) {
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    public static <T> void waitCompletionAndClear(List<CompletableFuture<T>> futures) {
        if (Util.isNotEmptyCollection(futures)) {
            waitCompletion(futures);
            futures.clear();
        }
    }



    public static void addExpectedTaskCount(int delta) {
        NEXT_BOOK_LOCK.lock();
        try {
            expectedTaskCount.addAndGet(delta);
            TASK_COUNT_SET_CONDITION.signalAll();
        } finally {
            NEXT_BOOK_LOCK.unlock();
        }
    }

    public static void signalTaskCountSet() {
        addExpectedTaskCount(0);
    }

    public static long getExpectedTaskCount() {
        return expectedTaskCount.get();
    }

    public static void nextBookAwait() {
        NEXT_BOOK_LOCK.lock();
        try {
            TASK_COUNT_SET_CONDITION.awaitUninterruptibly();  // do not place inside loop
            while (nextBookShouldWait()) {
                PROCEED_NEXT_BOOK_CONDITION.awaitUninterruptibly();
            }
        } finally {
            NEXT_BOOK_LOCK.unlock();
        }
    }

    public static void checkNextBookGo() {
        NEXT_BOOK_LOCK.lock();
        try {
            PROCEED_NEXT_BOOK_CONDITION.signalAll();
        } finally {
            NEXT_BOOK_LOCK.unlock();
        }
    }

    public static boolean nextBookShouldWait() {
        ThreadPoolExecutor bookExecutor = (ThreadPoolExecutor) getBookExecutor();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) getMainExecutor();

        long expectedTaskCount = getExpectedTaskCount();
        long currentTaskCount = executor.getTaskCount();
        int corePoolSize = executor.getCorePoolSize();
        int activeCount = executor.getActiveCount();

        int bookCorePoolSize = bookExecutor.getCorePoolSize();
        int bookActiveCount = bookExecutor.getActiveCount();

        boolean toGo = /*expectedTaskCount != 0
                &&*/ (currentTaskCount == expectedTaskCount)
                && (activeCount < LOAD_FACTOR * corePoolSize)
                && (bookActiveCount < bookCorePoolSize);

        return !toGo;
    }
}


package com.cchis.dctm.util.export.report;

import com.cchis.dctm.util.export.LogHandler;
import com.cchis.dctm.util.export.exception.ExportException;
import com.cchis.dctm.util.export.util.Util;
import org.apache.commons.lang.SerializationUtils;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.Arrays;

import static com.cchis.dctm.util.export.util.ExportConstants.EMPTY_STRING;


abstract class AbstractCount implements Serializable {
    private static final Logger LOG = Logger.getLogger(AbstractCount.class);
    private static final String MSG_WARN_DIFFERENT_SIZES = "sum(): AbstractCounts have different sizes: %d %d";

    protected int[] counts;
    protected Boolean sound;

    AbstractCount(int size) {
        counts = createEmptyCounts(size);
        sound = true;
    }

    AbstractCount(int[] counts, boolean isSound) {
        this(counts.length);
        System.arraycopy(counts, 0, this.counts, 0, counts.length);
        this.sound = isSound;
    }

    private int[] createEmptyCounts(int size) {
        int[] emptyCounts = new int[size];
        Arrays.fill(emptyCounts, 0);
        return emptyCounts;
    }

    public boolean isEmpty() {
        return Arrays.equals(counts, createEmptyCounts(getSize()));
    }

    public int[] getCounts() {
        return counts;
    }

    public boolean isSound() {
        return sound;
    }

    public void setCounts(int[] counts) {
        this.counts = counts;
    }

    public void setSound(boolean sound) {
        this.sound = sound;
    }

    public int getSize() {
        return counts.length;
    }

    @Override
    public String toString() {
        String toString = Arrays.toString(counts);
        return Util.commaJoinStrings(Util.simpleArrayToStringAsCSV(toString), sound);
    }

    public AbstractCount sum(final AbstractCount anotherRecord) {
        if (anotherRecord == null) return this;
        int size = getSize();
        int anotherSize = anotherRecord.getSize();
        if (size != anotherSize) {
            LOG.warn(String.format(MSG_WARN_DIFFERENT_SIZES, size, anotherSize));
            return null;
        }
        int[] counts2 = anotherRecord.getCounts();
        int[] result = new int[size];
        Arrays.setAll(result, i -> counts[i] + counts2[i]);
        boolean resultSound = sound && anotherRecord.isSound();
        setCounts(result);
        setSound(resultSound);
        return this;
    }

    public static AbstractCount copyOf(final AbstractCount count) {
        try {
            return (AbstractCount) SerializationUtils.clone(count);
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, "copyOf(): Cannot copy: " + count, e);
            throw new ExportException("copyOf", e);
        }
    }

    static final class Null extends AbstractCount {
        private int nullSize;

        private Null(int size) {
            super(size);
            nullSize = size;
            counts = null;
            sound = null;
        }

        public static Null getInstance(int size) {
            return new Null(size);
        }

        @Override
        public String toString() {
            String[] empties = new String[nullSize + 1];
            Arrays.fill(empties, EMPTY_STRING);
            return Util.commaJoinStrings((Object[])empties);
        }

        @Override
        public void setCounts(int[] counts) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setSound(boolean sound) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getSize() {
            return nullSize;
        }

        @Override
        public AbstractCount sum(AbstractCount anotherRecord) {
            throw new UnsupportedOperationException();
        }
    }
}

package com.cchis.dctm.util.export.report;

import java.util.ArrayList;
import java.util.List;

/**
 * All direct (for debugging)
 * All
 * Wrong format
 * Correct format
 * Published
 * Not Published
 * Duplicate Name
 * Defect Duplicate Name
 * Unknown Problem
 * Ready To Import
 * Sound?
 */
class CountRecord extends AbstractCount {
    private static final int COUNT_SIZE = 10;

    CountRecord() {
        super(COUNT_SIZE);
    }

    CountRecord(int[] counts, boolean sound) {
        super(counts, sound);
    }

    static List<String> getHeaders() {
        List<String> headers = new ArrayList<>(COUNT_SIZE + 1);
        headers.add("All (direct)");
        headers.add("All (cumulative)");
        headers.add("Wrong Format");
        headers.add("Correct Format");
        headers.add("Published");
        headers.add("Not Published");
        headers.add("Duplicate Name");
        headers.add("Defect Duplicate Name");
        headers.add("Unknown Problem");
        headers.add("Ready To Import");
        headers.add("Sound?");
        return headers;
    }

}

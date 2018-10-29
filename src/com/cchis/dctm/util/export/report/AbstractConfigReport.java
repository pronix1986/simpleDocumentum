package com.cchis.dctm.util.export.report;

import java.util.ArrayList;
import java.util.List;


abstract class AbstractConfigReport extends AbstractReport {

    protected static final String UNSOUND = "!Unsound!";

    @Override
    public List<String> getReportLines() {
        List<String> lines = new ArrayList<>(getCountReport());
        if (isRecordsReportNeeded()) {
            lines.addAll(getRecordsReport());
        }
        return lines;
    }

    protected boolean isRecordsReportNeeded() {
        return true;
    }

    protected abstract List<String> getCountReport();

    protected abstract List<String> getRecordsReport();



}

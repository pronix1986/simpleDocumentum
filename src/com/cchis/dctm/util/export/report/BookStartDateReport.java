package com.cchis.dctm.util.export.report;

import com.cchis.dctm.util.export.util.Util;
import org.apache.commons.lang.time.DateFormatUtils;

import java.util.*;

import static com.cchis.dctm.util.export.util.ExportConstants.EMPTY_STRING;

public class BookStartDateReport extends AbstractReport implements IReport {

    private static final BookStartDateReport INSTANCE = new BookStartDateReport();
    private final Map<String, String> startDates = new TreeMap<>();

    private BookStartDateReport() { }

    public static BookStartDateReport getInstance() {
        return INSTANCE;
    }

    @Override
    public List<String> getReportLines() {
        List<String> lines = new ArrayList<>();
        startDates.forEach((key, value) -> lines.add(Util.commaJoinStrings(key, value, EMPTY_STRING)));
        return lines;
    }

    @Override
    public void processReport(IReport iReport) {
        // empty
    }

    public void setStartDate(String configName) {
        startDates.put(configName,
                DateFormatUtils.ISO_DATETIME_FORMAT.format(new Date()));
    }



}

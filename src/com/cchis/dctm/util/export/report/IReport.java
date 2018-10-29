package com.cchis.dctm.util.export.report;

public interface IReport {
    String getReport();

    void logReport();

    void writeReport();

    void processReport(IReport report);
}

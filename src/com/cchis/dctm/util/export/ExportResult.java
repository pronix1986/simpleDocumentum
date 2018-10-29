package com.cchis.dctm.util.export;

import com.cchis.dctm.util.export.report.ReportRecord;

import java.util.List;

public class ExportResult {
    private List<ReportRecord> allReportRecords;
    private List<ReportRecord> readyToImportRecords;
    private int zipCount;

    public ExportResult() {
    }

    public ExportResult(List<ReportRecord> allReportRecords, List<ReportRecord> readyToImportRecords, int zipCount) {
        this.allReportRecords = allReportRecords;
        this.readyToImportRecords = readyToImportRecords;
        this.zipCount = zipCount;
    }

    public List<ReportRecord> getReadyToImportRecords() {
        return readyToImportRecords != null ? readyToImportRecords : getReadyToImportRecordsFromAllRecords();
    }

    public int getZipCount() {
        return zipCount;
    }

    public List<ReportRecord> getAllReportRecords() {
        return allReportRecords;
    }

    public List<ReportRecord> getReadyToImportRecordsFromAllRecords() {
        return allReportRecords != null ? ReportRecord.getReadyToImport(allReportRecords) : null;
    }

    public void setAllReportRecords(List<ReportRecord> allReportRecords) {
        this.allReportRecords = allReportRecords;
    }

    public void setReadyToImportRecords(List<ReportRecord> readyToImportRecords) {
        this.readyToImportRecords = readyToImportRecords;
    }

    public void setZipCount(int zipCount) {
        this.zipCount = zipCount;
    }
}

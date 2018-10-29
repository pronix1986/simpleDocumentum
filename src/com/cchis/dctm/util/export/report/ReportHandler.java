package com.cchis.dctm.util.export.report;

import com.cchis.dctm.util.export.LogHandler;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReportHandler {

    private static final Logger LOG = Logger.getLogger(ReportHandler.class);

    private final List<IReport> reports = new ArrayList<>();

    private static final ReportHandler INSTANCE = new ReportHandler();

    private ReportHandler() { }

    public static ReportHandler getInstance() {
        return INSTANCE;
    }

    public void addReport(IReport report) {
        reports.add(report);
    }

    public void addReports(IReport[] reports) {
        this.reports.addAll(Arrays.asList(reports));
    }

    public void processReport(IReport report) {
        for (IReport r : reports) {
            try {
                r.processReport(report);
            } catch(Exception e) {
                LOG.warn(String.format("Error in %s during processing %s", r, report));
            }
        }
    }

    public synchronized void processAndWriteReport(IReport report) {
        LOG.trace("processReport(): " + ((ConfigReport)report).getcId());
        for (IReport r : reports) {
            try {
                r.processReport(report);
                r.writeReport();
            } catch(Exception e) {
                LogHandler.logWithDetails(LOG, String.format("Error in %s during processing %s", r, report), e);
            }
        }
    }

    public void logReports() {
        for (IReport r : reports) {
            r.logReport();
        }
    }

    public void writeReports() {
        for (IReport r : reports) {
            r.writeReport();
        }
    }

    public void logAndWriteReports() {
        for (IReport r : reports) {
            ((AbstractReport)r).logAndWriteReport();
        }
    }
}

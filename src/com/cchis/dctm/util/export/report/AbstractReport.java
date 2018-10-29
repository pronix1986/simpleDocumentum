package com.cchis.dctm.util.export.report;

import com.cchis.dctm.util.export.PropertiesHolder;
import com.cchis.dctm.util.export.util.FileUtil;
import com.cchis.dctm.util.export.util.Util;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.File;

import java.util.List;


import static com.cchis.dctm.util.export.util.ExportConstants.*;

public abstract class AbstractReport implements IReport {
    private static final Logger LOG = Logger.getLogger(AbstractReport.class);

    protected static final String REPORT_EXT = ".csv";
    protected static final String REPORT_FORMAT = "%s : %n%s";

    public abstract List<String> getReportLines();

    public String getReport() {
        return Util.listOfObjectsToString(getReportLines());
    }

    protected String getReportName() {
        return this.getClass().getSimpleName();
    }

    protected String getReportPath() {
        return getReportPath(null, getReportName());
    }

    protected String getReportPath(String subfolder, String reportName) {
        return getReportPath(subfolder, reportName, REPORT_EXT, true);
    }

    public static String getReportPath(String subfolder, String reportName, String extension, boolean appendTimestamp) {
        String startDateTime = PropertiesHolder.getInstance().getProperty(PROP_START_DATE_TIME);
        StringBuilder builder = new StringBuilder(REPORT_FOLDER_PATH + File.separator);
        if (StringUtils.isNotEmpty(subfolder)) {
            builder.append(subfolder + File.separator);
        }
        builder.append(reportName);
        if (appendTimestamp) {
            builder.append(UNDERSCORE + startDateTime);
        }
        if (StringUtils.isNotEmpty(extension)) {
            builder.append(extension);
        }
        return builder.toString();
    }


    @Override
    public void logReport() {
        logReport(getReportName(), getReport());
    }

    @Override
    public void writeReport() {
        writeReport(getReportPath(), getReport());
    }

    public static void logReport(String reportName, String report) {
        if (StringUtils.isNotEmpty(report)) {
            LOG.debug(String.format(REPORT_FORMAT, reportName, report));
        }
    }

    public static void writeReport(String reportPath, String report) {
        if (StringUtils.isNotEmpty(report)) {
            FileUtil.writeStringToFileSilently(new File(reportPath), report);
        }
    }

    public void logAndWriteReport() {
        String report = getReport();
        logReport(getReportName(), report);
        writeReport(getReportPath(), report);
    }

}

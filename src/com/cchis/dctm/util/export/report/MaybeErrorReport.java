package com.cchis.dctm.util.export.report;

import com.cchis.dctm.util.export.ConfigId;
import com.cchis.dctm.util.export.util.Util;
import org.apache.commons.collections.ListUtils;

import java.util.ArrayList;
import java.util.List;

import static com.cchis.dctm.util.export.util.ExportConstants.NEW_LINE;

public class MaybeErrorReport extends AbstractReport {

    private static final MaybeErrorReport INSTANCE = new MaybeErrorReport();

    public static MaybeErrorReport getInstance() {
        return INSTANCE;
    }

    private final List<ConfigId> errorConfigs = new ArrayList<>(0);
    private final List<ConfigId> maybeErrorConfigs = new ArrayList<>(0);


    @Override
    public List<String> getReportLines() {
        List<String> lines = new ArrayList<>();
        if (Util.isEmptyCollection(ListUtils.union(errorConfigs, maybeErrorConfigs))) {
            return lines;
        }
        lines.add("Errors:");
        errorConfigs.forEach(cId -> lines.add(cId.toCSVString()));
        lines.add(NEW_LINE);
        lines.add("Maybe Errors:");
        maybeErrorConfigs.forEach(cId -> lines.add(cId.toCSVString()));
        return lines;
    }

    @Override
    public void processReport(IReport iReport) {
        ConfigReport report = (ConfigReport) iReport;
        errorConfigs.addAll(report.getErrorConfigId());
        maybeErrorConfigs.addAll(report.getMaybeErrorConfigId());
    }
}

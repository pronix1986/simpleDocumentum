package com.cchis.dctm.util.export.report;

import com.cchis.dctm.util.export.*;
import com.cchis.dctm.util.export.util.FileUtil;
import com.cchis.dctm.util.export.util.Util;
import com.documentum.admin.object.IDfWebCacheConfig;
import org.apache.commons.collections.ListUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;

import static com.cchis.dctm.util.export.util.ExportConstants.*;

public class DuplicateNamesReport extends AbstractConfigReport {
    private static final Logger LOG = Logger.getLogger(DuplicateNamesReport.class);
    public static final String DUP_NAME_PATH = REPORT_FOLDER_PATH + File.separator + DUPLICATE_NAME;

    private static final DuplicateNamesReport INSTANCE = new DuplicateNamesReport();

    private final List<ReportRecord> readyToImportDuplicateNames = new ArrayList<>();

    private final Map<String, AbstractCount> configCounts = new LinkedHashMap<>();
    private final Map<String, AbstractCount> summaryCounts = new LinkedHashMap<>();

    public static DuplicateNamesReport getInstance() {
        return INSTANCE;
    }

    private DuplicateNamesReport() { }

    private String getHeader() {
        return Util.commaJoinStrings("Name",
                Util.simpleArrayToStringAsCSV(CountDuplicateNames.getHeaders().toString()));
    }

    @Override
    protected List<String> getCountReport() {
        List<String> lines = new ArrayList<>();
        AbstractCount totalCount = summaryCounts.get(TOTAL);
        if (totalCount == null || totalCount.isEmpty()) {
            return lines;
        }
        configCounts.forEach((configName, configCount) -> lines.add(Util.commaJoinStrings(configName, configCount)));
        Collections.sort(lines);
        lines.add(Util.wrap(TOTAL, " --- "));
        lines.add(Util.commaJoinStrings(TOTAL, totalCount));
        if (!totalCount.isSound()) {
            lines.add(UNSOUND);
        }
        lines.add(0, getHeader());
        return lines;
    }

    @Override
    protected List<String> getRecordsReport() {
        List<String> lines = new ArrayList<>();
        if (Util.isNotEmptyCollection(readyToImportDuplicateNames)) {
            lines.add("Ready to Import");
            lines.add(Util.listOfObjectsToString(readyToImportDuplicateNames));
        }
        return lines;
    }

    @Override
    public void processReport(IReport iReport) {
        ConfigReport report = (ConfigReport) iReport;
        AbstractCount countRecord = exportDuplicateNames(report);
        String configName = report.getcId().getConfigName();
        configCounts.put(configName, countRecord);
        summaryCounts.merge(TOTAL, AbstractCount.copyOf(countRecord), AbstractCount::sum);
    }

    @SuppressWarnings("unchecked")
    private AbstractCount exportDuplicateNames(ConfigReport report) {
        if (report.isSubconfig() || (report.getDuplicateNameCount() == 0)
                 && (report.getDefectDuplicateNamesCount() == 0)) return new CountDuplicateNames();
        LOG.trace("Export Duplicate Names");
        List<ReportRecord> allDefectNames = ListUtils.union(report.getDuplicateNames(), report.getDefectDuplicateNames());
        LOG.trace("allDefectNames: " + allDefectNames);
        List<String> ids = new ArrayList<>();
        for (ReportRecord record : allDefectNames) {
            ids.add(record.getId());
            ids.addAll(record.getDuplicateNameIds());
        }
        LOG.trace("ids to export: " + ids);

        IDfWebCacheConfig config = report.getConfig();
        ConfigId cId = report.getcId();
        String partConfigId = cId.getConfigId().substring(8);
        String root = DUP_NAME_PATH + File.separator + EXPORT_SET_FOLDER_PREFIX + partConfigId;
        File exportSet = new File(root);

        Function<ReportRecord, String> nameMap = record -> {
            String docName = record.getFilteredObjectNameNullSafe();
            String id = record.getId();
            return id + UNDERSCORE + docName;
        };

        String fullZipFileName = WebcHandler.createNameForConfigWithVersion(cId.getConfigName(), null, null,
                DUPLICATE_NAME_POSTFIX) + ZIP_EXT;
        File zipFolder = WebcHandler.getZipFolder(cId.getConfigName());
        File zipFile = new File(zipFolder, fullZipFileName);
        Path zipPath = zipFile.toPath();

        ExportResult exportResult = FileUtil.exportAndZipFilesUnderBook(config, ids, nameMap, exportSet.toPath(), zipPath);
        List<ReportRecord> currentReadyToImport = exportResult.getReadyToImportRecords();
        int count = exportResult.getZipCount();

        readyToImportDuplicateNames.addAll(currentReadyToImport);
        if (count != ids.size()) {
            LOG.warn(String.format("count != ids.size(): %s %s", count, ids.size()));
        }
        int notExported = ids.size() - count;
        WebcHandler.deleteExportSets(Collections.singletonList(exportSet), cId);

        return new CountDuplicateNames(new int[]{
                report.getDuplicateNameCount(), report.getDefectDuplicateNamesCount(),
                ids.size(), count, notExported, currentReadyToImport.size()
        }, (notExported == 0));
    }
}

class CountDuplicateNames extends AbstractCount {
    private static final int COUNT_SIZE = 6;

    CountDuplicateNames() {
        super(COUNT_SIZE);
    }

    CountDuplicateNames(int[] counts, boolean sound) {
        super(counts, sound);
    }

    static List<String> getHeaders() {
        List<String> headers = new ArrayList<>(COUNT_SIZE + 1);
        headers.add("Duplicate Name");
        headers.add("Defect Duplicate Name");
        headers.add("Affected Records");
        headers.add("Exported");
        headers.add("Not Exported");
        headers.add("Ready To Import");
        headers.add("Sound?");
        return headers;
    }
}
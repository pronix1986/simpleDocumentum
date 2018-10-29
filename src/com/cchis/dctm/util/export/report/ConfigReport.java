package com.cchis.dctm.util.export.report;

import com.cchis.dctm.util.export.*;
import com.cchis.dctm.util.export.exception.ExportException;
import com.cchis.dctm.util.export.util.*;
import com.documentum.admin.object.IDfWebCacheConfig;
import com.documentum.fc.client.*;
import com.documentum.fc.common.DfException;
import com.documentum.wcm.type.WcmContent;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.collections.PredicateUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.cchis.dctm.util.export.util.ExportConstants.*;

/**
 * isExportSetValid - export set count > 0 and no 'Unknown Problem' records
 *
 */
public class ConfigReport extends AbstractConfigReport {
    private static final Logger LOG = Logger.getLogger(ConfigReport.class);


    private static final String MSG_WARN_EXCESSIVE_PUBLISHING = "Excessive publishing. Something went wrong. DCTM count: %s, publishedCount: %s";

    private boolean isExportSetValid = true;

    private IDfWebCacheConfig config;
    private ConfigId cId;
    private String version;

    private int dctmCountDirect = 0;
    private int dctmCount = 0;
    private int publishedCount = 0;
    private int readyToImportCount = 0;

    private List<ReportRecord> notPublished;

    private List<ReportRecord> duplicateNames;
    private List<ReportRecord> defectDuplicateNames;
    private List<ReportRecord> unknownProblem;

    // public modifier is used intenti
    public List<ReportRecord> defectNames;
    public List<ReportRecord> wrongFormat;
    public List<ReportRecord> readyToImport;

    private List<ConfigReport> configReports;
    private List<ConfigId> errorConfigId;
    private List<ConfigId> maybeErrorConfigId;

    private boolean isTotal = false;

    public List<ConfigId> getErrorConfigId() {
        return errorConfigId;
    }

    public void setErrorConfigId(List<ConfigId> errorConfigId) {
        this.errorConfigId = errorConfigId;
    }

    public List<ConfigId> getMaybeErrorConfigId() {
        return maybeErrorConfigId;
    }

    public void setMaybeErrorConfigId(List<ConfigId> maybeErrorConfigId) {
        this.maybeErrorConfigId = maybeErrorConfigId;
    }

    private static final IReport INSTANCE = new ConfigReport().new TotalReport();

    public static IReport getTotalReportInstance() {
        return INSTANCE;
    }

    private ConfigReport() { }

    /**
     * 'Parent' config constructor
     * @param config
     */
    public ConfigReport(IDfWebCacheConfig config) {
        try {
            this.config = config;
            this.cId = ConfigId.getConfigId(config);
            this.configReports = Collections.synchronizedList(new ArrayList<>()); // no remove operation
            this.dctmCountDirect = DocumentHandler.countDocumentsUnderBook(config);
        } catch (Exception e) {
            LOG.error(MSG_ERROR, e);
        }
    }

    /**
     * 'Child' config constructor
     * @param session
     * @param config
     * @param exportSets
     * @param exportSetCount
     */
    public ConfigReport(IDfSession session, IDfWebCacheConfig config, List<File> exportSets, int exportSetCount) {
        StopWatchEx stopWatch = StopWatchEx.createStarted();
        try {
            this.cId = ConfigId.getConfigId(config);
            LOG.trace("Start generating configReport for " + cId + ". Session: " + session.hashCode());
            initLists();
            this.configReports = null;
            this.publishedCount = exportSetCount;
            this.config = config;
            this.version = cId.getVersion();
            if (DETAILED_REPORT) {
                populateDetailedReport(session, exportSets, stopWatch);
            } else {
                this.dctmCount = DocumentHandler.countDocumentsUnderBookWithVersion(session, config, version);
            }
            if (exportSetCount > dctmCount) {
                LOG.warn(String.format(MSG_WARN_EXCESSIVE_PUBLISHING, dctmCount, exportSetCount));
            }
            logReport();
            // special case when no 'correct format' records in export set.
            this.isExportSetValid = (exportSetCount > 0 && getUnknownProblemCount() == 0) || (getWrongFormatCount() == getDctmCountDirect());
            if (!isExportSetValid) {
                processInvalidExportSet(exportSets, exportSetCount);
            }
            LOG.trace("ConfigReport for " + cId + " object creation time: " + stopWatch.stopAndGetTime());
        } catch (Exception e) {
            LOG.error(MSG_ERROR, e);
        }
    }

    private void processInvalidExportSet(List<File> exportSets, int exportSetCount) {
        LOG.warn(String.format(MSG_CONFIG_REPORT_INVALID, cId, exportSetCount, getUnknownProblemCount()));
        int esSize = exportSets.size();
        for (int i = 0; i < esSize; i++) {
            File destination;
            String basePath = REPORT_FOLDER_PATH + File.separator + INVALID_EXPORT_SETS + File.separator + cId.getConfigName();
            if (i == 0 && esSize == 1) {
                destination = new File(basePath);
            } else {
                destination = new File(basePath + UNDERSCORE + i);
            }
            FileUtil.copyDirectorySilently(exportSets.get(i), destination);
        }
    }

    private void findDefectDuplicateNames() {
        if (Util.isNotEmptyCollection(unknownProblem)
                || Util.isNotEmptyCollection(defectNames)) {
            LOG.trace("unknownProblem: " + unknownProblem);
            LOG.trace("defectNames: " + defectNames);
            LOG.trace("defectDuplicateNames: " + defectDuplicateNames);

            // TODO: also have a case when defectDuplicateName may override normal record. Is it reasonable to implement?
            Map<String, List<ReportRecord>> filteredPathsMap = ReportRecord.getFilteredPathsMap(defectNames);

            filteredPathsMap.forEach((key, defectRecords) -> {
                if (defectRecords.size() < 2) return;
                ReportRecord firstRecord = defectRecords.get(0);
                if (firstRecord.getDuplicateNameIds() == null) {
                    firstRecord.setDuplicateNameIds(new ArrayList<>());
                }

                defectDuplicateNames.add(firstRecord);
                defectRecords.stream().skip(1).forEach(record -> {
                    firstRecord.getDuplicateNameIds().add(record.getId());
                    unknownProblem.remove(record);
                });
            });
        }
    }

    public boolean isExportSetValid() {
        if (isSubconfig()) return isExportSetValid;
        for (ConfigReport rr : configReports) {
            if (!rr.isExportSetValid()) return false;
        }
        return true;
    }

    public boolean isSound() {
        //LOG.trace(String.format("Report %s isExportSetValid? : %s", cId, isExportSetValid()));
        //LOG.trace(String.format("Report %s isNotBalanced? : %s", cId, isNotBalanced()));
        return isExportSetValid() && !isNotBalanced();
    }

    // for async publishing or double-check if PublishResult.preserveResponse() fails
    public void preserveResponses() {
        LOG.trace("isSubconfig?: " + isSubconfig());
        if (isSubconfig()) return;
        LOG.trace(String.format("Report %s isSound / Valid / Balanced? : %s/%s/%s", cId, isSound(), isExportSetValid(), !isNotBalanced()));
        if (!isSound()) {
            LOG.trace("Not balanced or invalid");
            File source = IDS_RESPONCE_DIR_FILE;
            if (!FileUtil.isDirEmpty(source)) {
                String configName = WebcHandler.getConfigNameWithoutVersion(cId.getConfigName());
                String reportPath = getReportPath(BACKED_RESPONSES, configName, null, true);
                File destination = new File(reportPath);
                FileUtil.copyDirectorySilently(source, destination);
            }
        }
    }

    private boolean isPreciselyBalanced() {
        return getDctmCount() == getPublishedCount();
    }

    private void initLists() {
        this.notPublished = new ArrayList<>(0);
        this.wrongFormat = new ArrayList<>(0);
        this.duplicateNames = new ArrayList<>(0);
        this.defectDuplicateNames = new ArrayList<>(0);
        this.unknownProblem = new ArrayList<>(0);
        this.defectNames = new ArrayList<>(0);
        this.readyToImport = new ArrayList<>();
    }

    public boolean isSubconfig() {
        return configReports == null;
    }

    private void populateDetailedReport(IDfSession session, List<File> exportSets, StopWatchEx stopWatch) {
        try {
            populateConfigReport(session, exportSets);
            LOG.trace("ConfigReport for " + cId + ". Previous + populateConfigReport time taken: " + stopWatch.getTime());
            findDefectDuplicateNames();
            LOG.trace("ConfigReport for " + cId + ". Previous + finding defect duplicates time taken: " + stopWatch.getTime());
            LOG.trace("defectDuplicateNames: " + defectDuplicateNames);
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, Level.WARN, String.format(MSG_ERROR_GET_RECORD_COUNT_REPORT, cId), e);
        }
    }

    private void populateConfigReport(IDfSession session, List<File> exportSets) {
        StopWatchEx stopWatch = StopWatchEx.createStarted();
        LOG.trace("ConfigReport for " + cId + ". Start populateConfigReport()");
        IDfCollection collection = null;
        try {
            String folderPath = WebcHandler.getPublishFolder(session, config);
            List<String> exportSetFiles = FileUtil.getExportSetsFiles(exportSets, folderPath);
            int dctmCountInLoop = 0;

            //if(!WebcHandler.isWebcSlow()) {
                String dql = String.format(DQL_GET_DOCUMENTS_WITH_VERSION, WebcHandler.getPublishType(config), folderPath, version);
                dql = PublishType.adjustDql(dql, config);
                dql = Util.appendDqlHints(dql, DQL_HINT_UNCOMMITTED_READ, DQL_HINT_SQL_DEF_RESULT_SETS);
                LOG.trace(dql);
                collection = Util.runQuery(session, dql);
                LOG.trace("ConfigReport for " + cId + ". Execute GET_NAMES time taken: " + stopWatch.getTime());

                while (collection.next()) {
                    dctmCountInLoop++;
                    ReportRecord record = new ReportRecord(session, collection.getId(DCTM_R_OBJECT_ID), config);
                    processRecord(session, record, exportSetFiles);
                }
            //} else {
            //    List<ReportRecord> records = SlowDqlReportExecutorEx.getCurrent().getReportRecords(version);
            //    for (ReportRecord record : records) {
            //        dctmCountInLoop++;
            //        processRecord(session, record, exportSetFiles);
            //    }
            //}

            if (Util.isNotEmptyCollection(exportSetFiles)) {
                LOG.trace("exportSet files remainders: " + exportSetFiles);
            }
            dctmCount = dctmCountInLoop;
        } catch (Exception e) {
            LOG.error(MSG_ERROR, e);
            throw new ExportException(e.getMessage());
        } finally {
            Util.closeCollection(collection);
            LOG.trace("ConfigReport for " + cId + ". End populatingConfigReport(). Time taken: " + stopWatch.stopAndGetTime());
        }
    }

    private void processRecord(IDfSession session, ReportRecord record, List<String> exportSetFiles) throws DfException {
        String folderPath = WebcHandler.getPublishFolder(session, config);
        //boolean isPreciselyBalanced = isPreciselyBalanced();

        // some images are linked to several folders.
        // Workaround: path = folderPath
        if (!StringUtils.containsIgnoreCase(record.getFolderPath(), folderPath)) {
            LOG.trace(String.format("Path != Folder Path. Config %s folder path: %s. File path: %s, name: %s",
                    cId, folderPath, record.getFolderPath(), record.getObjectName()));
            record.setFolderPath(folderPath);
        }

        String oldPath = record.getPath();

        if (/*!isPreciselyBalanced &&*/ !record.getFullFormat().equals(WebcHandler.getPublishFormat(cId.getConfigName()))) {
            wrongFormat.add(record);
            if (getRecordNameCount(session, record) < 2) { // we can have duplicate name records, one of them has wrong format: CA MIN 2009
                exportSetFiles.remove(record.getPath());
            }
            return;
        }

        processReadyToImport(record);

        filterRecord(record);
        String newPath = record.getFilteredPathNullSafe();
        String newName = record.getFilteredObjectNameNullSafe();

        //if (!isPreciselyBalanced) {
            // Should be case insensitive. There's an issue with Windows file system case insensitivity.
            // See /icspipeline.com/MO/INSource/SIC/TITLE_XXIV/Chapter_383/ Medical_MalPractice and Medical_Malpractice
            Predicate equalsIgnoreCasePredicate = EqualsIgnoreCasePredicate.getInstance(newPath);
            if (!CollectionUtils.exists(exportSetFiles, equalsIgnoreCasePredicate)) {
                LOG.trace(
                        String.format("Not in exportSet. oldPath: %s, newPath: %s, oldName: %s, newName: %s, record: %s, exportSetFiles: %s",
                                oldPath, newPath, record.getObjectName(), newName, record, exportSetFiles));
                findDuplicateNames(session, record);
            } else {
                CollectionUtils.filter(exportSetFiles, PredicateUtils.notPredicate(equalsIgnoreCasePredicate));
            }
        //}
    }

    private void filterRecord(ReportRecord record) {
        String oldPath = record.getPath();
        boolean filtered = filterRecordPath(record);
        String newPath = record.getFilteredPathNullSafe();

        if (filtered) {
            LOG.trace("oldPath: " + oldPath + ". newPath: " + newPath + ". Record: " + record);
            if (ReportRecord.getPaths(defectNames).contains(oldPath)) {
                LOG.warn("Duplicate in defectNames: " + oldPath);
            }
            defectNames.add(record);
        }
    }

    private void processReadyToImport(ReportRecord record) throws DfException {
        if (record.isReadyToImport()) {
            readyToImport.add(record);
        }
    }

    private void findDuplicateNames(IDfSession session, ReportRecord record) {
        String newName = record.getFilteredObjectNameNullSafe();
        notPublished.add(record);

        String template = getTemplateName(record);

        IDfCollection idCollection = null;
        try {
            int count = getRecordNameCount(session, record);
            if (count > 2) {
                LOG.warn(String.format("count = %d > 2. Probably something went wrong", count));
            }
            if (count > 1) {
                List<String> ids = new ArrayList<>();
                String dupDql = String.format(DQL_GET_DUPLICATE_NAMES_IDS, WebcHandler.getPublishType(config),
                        record.getFolderPath(), version, newName, template, record.getId());
                LOG.trace(dupDql);
                idCollection = Util.runQuery(session, dupDql);
                while (idCollection.next()) {
                    ids.add(idCollection.getString(DCTM_R_OBJECT_ID));
                }
                record.setDuplicateNameIds(ids);
                LOG.trace("DuplicateNameIds: " + ids);
                duplicateNames.add(record);
                return;
            }
        } catch (Exception e) {
            LOG.error(MSG_ERROR, e);
            throw new ExportException(e.getMessage());
        } finally {
            Util.closeCollection(idCollection);
        }
        unknownProblem.add(record);
    }

    private String getTemplateName(String name) {
        String template = name;
        if (name.lastIndexOf(DOT) != -1) {
            template = name.substring(0, name.lastIndexOf(DOT));
        }
        return template;
    }

    private String getTemplateName(ReportRecord record) {
        String name = record.getObjectName();
        return getTemplateName(name);
    }

    private int getRecordNameCount(IDfSession session, ReportRecord record) {
        String recordFolderPath = record.getFolderPath();
        String name = record.getFilteredObjectNameNullSafe();
        String template = getTemplateName(record);
        try {
            String dql = String.format(DQL_GET_DUPLICATE_NAMES_COUNT, WebcHandler.getPublishType(config), recordFolderPath, version, name, template);
            int count = Util.getCount(session, dql);
            LOG.trace(String.format("folderPath: %s, name: %s, template: %s, dql: %s. %ncount: %d", recordFolderPath, name, template, dql, count));
            return count;
        } catch (Exception e) {
            LOG.error(MSG_ERROR, e);
            throw new ExportException(e.getMessage());
        }
    }

    private static String correctExtension(String name, String[] errExts, String extension) {
        for (String errExt : errExts) {
            if (name.endsWith(errExt)) {
                String newName = name.replace(errExt, extension);
                LOG.trace(String.format("correctExtension(). Old Name: %s, New Name: %s", name, newName));
                return newName;
            }
        }
        return name;
    }

    public boolean filterRecordPath(ReportRecord record) {
        return filterRecordPath(cId, record);
    }

    public static boolean filterRecordPath(ConfigId cId, ReportRecord record) {
        String name = record.getObjectName();
        String oldName = name;
        String folderPath = record.getFolderPath();

        // the most reliable way is to use IWcmContent::getPublishedName, but it may degrade performance
        // name = record.getPublishedName()
        switch (WebcHandler.getWebcType(cId.getConfigName())) {
            case INSIGHT:
                // Documentum corrects wrong doc ext
                String[] errExts = new String[] {
                        "._html", /*".html.",*/ ".htmhtm", ".htmll", ".htm_", ".html_", ".tml"
                };
                name = correctExtension(name, errExts, DEFAULT_INSIGHT_EXT);
                if (!name.equals(oldName)) {
                    break;
                }

                name = correctName(name, DEFAULT_INSIGHT_EXT, ".html", ".htm", ".doc", ".docx", ".pdf", ".unknown",
                        ".crtext", ".msw8", ".mid", ".tml");
                break;

            case INSOURCE:
                name = correctName(name, DEFAULT_INSOURCE_EXT, DEFAULT_INSOURCE_EXT);
                break;

            case IMAGE:
                name = correctName(name, DEFAULT_IMAGE_EXT, DEFAULT_IMAGE_EXT, ".gpeg");
                break;

            default: throw new IllegalArgumentException(MSG_ERROR_UNKNOWN_WEBC_TYPE);
        }
        // in docbase we have dirs like 'Part1.' and they transformed to 'Part1' in Windows.
        if (folderPath.contains("./")) {
            folderPath = folderPath.replaceAll("\\./", SLASH);
        }
        if (folderPath.endsWith(DOT)) {
            folderPath = folderPath.substring(0, folderPath.length() - 1);
        }
        // in docbase you can put slash in object name
        if (name.contains(SLASH)) {
            name = name.replaceAll(SLASH, UNDERSCORE);
        }

        return record.setFilteredPath(name, folderPath);
    }

    private static String correctName(String name, String defaultExt, String... allowedExts) {
        boolean endsWithAllowedExt = false;
        for (String allowedExt : allowedExts) {
            endsWithAllowedExt |= name.toLowerCase().endsWith(allowedExt);
        }
        if (!endsWithAllowedExt) {
            int index = name.lastIndexOf(DOT);
            return (index > 0) ? name.substring(0, index) + defaultExt : name + defaultExt;
        }
        return name;
    }

    public void processReport(IReport report) {
        if (report == null) return;
        ConfigReport configReport = (ConfigReport) report;
        configReports.add(configReport);
    }


    private int getDctmCount() {
        if (isSubconfig()) return dctmCount;
        if (dctmCount > 0) return dctmCount;
        dctmCount = configReports.stream().mapToInt(ConfigReport::getDctmCount).sum();
        return dctmCount;
    }


    private int getDctmCountDirect() {
        if (isSubconfig() && !isTotal) return dctmCount;
        return dctmCountDirect;
    }

    private int getPublishedCount() {
        if (isSubconfig()) return publishedCount;
        if (publishedCount > 0) return publishedCount;
        publishedCount = configReports.stream().mapToInt(ConfigReport::getPublishedCount).sum();
        return publishedCount;
    }

    private int getDctmCorrectFormatCount() { // ideal case = publishedCount
        return getDctmCount() - getWrongFormatCount();
    }

    private int getReadyToImportCount() {
        if (isSubconfig()) return readyToImportCount <= 0 ? readyToImport.size() : readyToImportCount;
        return configReports.stream().mapToInt(ConfigReport::getReadyToImportCount).sum();
    }

    private int getNotPublishedCount() {
        if (isSubconfig()) return notPublished.size();
        return configReports.stream().mapToInt(ConfigReport::getNotPublishedCount).sum();
    }

    private int getWrongFormatCount() {
        if (isSubconfig()) return wrongFormat.size();
        return configReports.stream().mapToInt(ConfigReport::getWrongFormatCount).sum();
    }

    public int getDuplicateNameCount() {
        if (isSubconfig()) return duplicateNames.size();
        return configReports.stream().mapToInt(ConfigReport::getDuplicateNameCount).sum();
    }

    public int getDefectDuplicateNamesCount() {
        if (isSubconfig()) {
            return defectDuplicateNames.stream().mapToInt(record -> record.getDuplicateNameIds().size()).sum();
        }
        return configReports.stream().mapToInt(ConfigReport::getDefectDuplicateNamesCount).sum();
    }

    private int getUnknownProblemCount() {
        if (isSubconfig()) return unknownProblem.size();
        return configReports.stream().mapToInt(ConfigReport::getUnknownProblemCount).sum();
    }

    private int getDefectNamesCount() {
        if (isSubconfig()) return defectNames.size();
        return configReports.stream().mapToInt(ConfigReport::getDefectNamesCount).sum();
    }

    // should be public
    public List<ReportRecord> getReadyToImport() {
        if (isSubconfig()) return readyToImport;
        if (readyToImport != null) return readyToImport;
        readyToImport = configReports.stream().map(ConfigReport::getReadyToImport)
                .flatMap(Collection::stream).collect(Collectors.toList());
        LOG.trace("getReadyToImport(). readyToImport: " + readyToImport);
        return readyToImport;
    }

    // should be public
    public List<InventoryRecord> getInventory() {
        List<InventoryRecord> irs = getReadyToImport().stream().map(InventoryRecord::new)
                .collect(Collectors.groupingBy(InventoryRecord::getChronicleId)).values()
                .stream().map(InventoryRecord::combineRecords).collect(Collectors.toList());
        irs.add(new InventoryCountRecord(getReadyToImport().size()));
        return irs;
    }

    List<ReportRecord> getNotPublished() {
        if (isSubconfig()) return notPublished;
        if (notPublished != null) return notPublished;
        notPublished = configReports.stream().map(ConfigReport::getNotPublished)
                .flatMap(Collection::stream).collect(Collectors.toList());
        LOG.trace("getNotPublished(). notPublished: " + notPublished);
        return notPublished;
    }

    // should be public
    public List<ReportRecord> getWrongFormat() {
        if (isSubconfig()) return wrongFormat;
        if (wrongFormat != null) return wrongFormat;
        wrongFormat = configReports.stream().map(ConfigReport::getWrongFormat)
                .flatMap(Collection::stream).collect(Collectors.toList());
        LOG.trace("getWrongFormat(). wrongFormat: " + wrongFormat);
        return wrongFormat;
    }

    List<ReportRecord> getDuplicateNames() {
        if (isSubconfig()) return duplicateNames;
        if (duplicateNames != null) return duplicateNames;
        duplicateNames = configReports.stream().map(ConfigReport::getDuplicateNames)
                .flatMap(Collection::stream).collect(Collectors.toList());
        LOG.trace("getDuplicateNames(). duplicateNames: " + duplicateNames);
        return duplicateNames;
    }

    List<ReportRecord> getDefectDuplicateNames() {
        if (isSubconfig()) return defectDuplicateNames;
        if (defectDuplicateNames != null) return defectDuplicateNames;
        defectDuplicateNames = configReports.stream().map(ConfigReport::getDefectDuplicateNames)
                .flatMap(Collection::stream).collect(Collectors.toList());
        LOG.trace("getDefectDuplicateNames(). defectDuplicateNames: " + defectDuplicateNames);
        return defectDuplicateNames;
    }

    List<ReportRecord> getUnknownProblem() {
        if (isSubconfig()) return unknownProblem;
        if (unknownProblem != null) return unknownProblem;
        unknownProblem = configReports.stream().map(ConfigReport::getUnknownProblem)
                .flatMap(Collection::stream).collect(Collectors.toList());
        LOG.trace("getUnknownProblem(). unknownProblem: " + unknownProblem);
        return unknownProblem;
    }

    public List<ReportRecord> getDefectNames() {
        if (isSubconfig()) return defectNames;
        if (defectNames != null) return defectNames;
        defectNames = configReports.stream().map(ConfigReport::getDefectNames)
                .flatMap(Collection::stream).collect(Collectors.toList());
        LOG.trace("getDefectNames(). defectNames: " + defectNames);
        return defectNames;
    }

    public IDfWebCacheConfig getConfig() {
        return config;
    }

    public CountRecord getCountRecord() {
        return new CountRecord(
                new int[] {
                        getDctmCountDirect(), getDctmCount(), getWrongFormatCount(),
                        getDctmCorrectFormatCount(), getPublishedCount(), getNotPublishedCount(),
                        getDuplicateNameCount(), getDefectDuplicateNamesCount(),
                        getUnknownProblemCount(), getReadyToImportCount()
                }, isSound());
    }

    protected List<String> getCountReport() {
        List<String> lines = new ArrayList<>();
        if (DETAILED_REPORT) {
            if (getDctmCountDirect() <= 0) {
                return lines;
            }
            lines.add(Util.simpleArrayToStringAsCSV(CountRecord.getHeaders().toString()));
            lines.add(Util.commaJoinStrings(getDctmCountDirect(),
                    getDctmCount(), getWrongFormatCount(), getDctmCorrectFormatCount(),
                    getPublishedCount(), getNotPublishedCount(),
                    getDuplicateNameCount(), getDefectDuplicateNamesCount(), getUnknownProblemCount(),
                    (readyToImportCount > 0 ? readyToImportCount : getReadyToImportCount()), isSound()));
            if (!isSound()) {
                lines.add(UNSOUND);
            }
        } else {
            lines.add(Util.commaJoinStrings("All(direct)", "All(cumulative)", "Published"));
            lines.add(Util.commaJoinStrings(getDctmCountDirect(), getDctmCount(), getPublishedCount()));
        }
        return lines;
    }

    protected List<String> getRecordsReport() {
        List<String> lines = new ArrayList<>();
        if (getNotPublishedCount() > 0) {
            lines.add("Not Published:");
            lines.add(Util.listOfObjectsToString(getNotPublished()));
        }
        if (getWrongFormatCount() > 0) {
            lines.add("Wrong Format:");
            lines.add(Util.listOfObjectsToString(getWrongFormat()));
        }
        if (getDuplicateNameCount() > 0) {
            lines.add("Duplicate Names:");
            lines.add(Util.listOfObjectsToString(getDuplicateNames()));
        }
        if (getDefectDuplicateNamesCount() > 0) {
            lines.add("Defect Duplicate Names");
            lines.add(Util.listOfObjectsToString(getDefectDuplicateNames()));
        }
        if (getDefectNamesCount() > 0) {
            lines.add("Defect Names:");
            lines.add(Util.listOfObjectsToString(getDefectNames()));
        }
        if (getUnknownProblemCount() > 0) {
            lines.add("Unknown Problem:");
            lines.add(Util.listOfObjectsToString(getUnknownProblem()));
        }
        return lines;
    }

    @Override
    protected boolean isRecordsReportNeeded() {
        return !getNotPublished().isEmpty() || !getDefectNames().isEmpty();
    }

    @Override
    public void logReport() {
        super.logReport();
        if (!isSound()) {
            LOG.warn(UNSOUND);
        }
    }

    public boolean isNotBalanced() {
        return (getDctmCount() != getDctmCountDirect())
                || (getDctmCount() - getWrongFormatCount() != getPublishedCount() + getNotPublishedCount())
                || (getNotPublishedCount() != getDuplicateNameCount() + getDefectDuplicateNamesCount() + getUnknownProblemCount());
    }

    public String getVersion() {
        return version;
    }

    public ConfigId getcId() {
        return cId;
    }

    @Override
    protected String getReportName() {
        return super.getReportName() + DASH + cId;
    }

    private class TotalReport extends AbstractConfigReport {

        private final List<IDfWebCacheConfig> configs;

        private TotalReport() {
            initLists();
            configs = new ArrayList<>();
            isTotal = true;
        }

        public synchronized void processReport(IReport iReport) {
            ConfigReport report = (ConfigReport) iReport;
            if (report == null || report.isSubconfig()) return;
            report.preserveResponses();

            configs.add(report.getConfig());
            dctmCount += report.getDctmCount();
            publishedCount += report.getPublishedCount();
            readyToImportCount += report.getReadyToImportCount();
            if(!report.isExportSetValid()) {
                isExportSetValid = false;
            }

            ReportRecord.addAllCopies(notPublished, report.getNotPublished());
            ReportRecord.addAllCopies(wrongFormat, report.getWrongFormat());
            ReportRecord.addAllCopies(duplicateNames, report.getDuplicateNames());
            ReportRecord.addAllCopies(defectDuplicateNames, report.getDefectDuplicateNames());
            ReportRecord.addAllCopies(unknownProblem, report.getUnknownProblem());
            ReportRecord.addAllCopies(defectNames, report.getDefectNames());

            // getters should have 'public' modifier
            writeConfigReport(report, "defectNames", ReportRecord.DUMMY_RECORD);
            writeConfigReport(report, "wrongFormat", ReportRecord.DUMMY_RECORD);
            writeConfigReport(report, "readyToImport", ReportRecord.DUMMY_RECORD);
            writeConfigReport(report, "inventory", InventoryRecord.DUMMY_RECORD);
        }

        @SuppressWarnings("unchecked")
        private void writeConfigReport(ConfigReport report, String reportProperty, ReportRecord header) {
            try {
                LOG.debug("Start writing report: " + reportProperty);
                String subfolder = Util.camelCaseToUnderscoreDelimited(reportProperty);
                String configName = report.getcId().getConfigName();
                List<ReportRecord> reportToWrite = new ArrayList<>((List<ReportRecord>)PropertyUtils.getProperty(report, reportProperty));
                if (Util.isNotEmptyCollection(reportToWrite)) {
                    File reportFile = new File(getReportPath(subfolder,
                            WebcHandler.getConfigNameWithoutVersion(configName)));
                    reportToWrite.add(0, header);
                    FileUtil.writeLinesSilently(reportFile, reportToWrite);
                }
            } catch (Exception e) {
                LogHandler.logWithDetails(LOG, Level.WARN, "writeConfigReport() : ", e);
            }
        }

        @Override
        public void logReport() {
            if (DETAILED_REPORT) {
                super.logReport();
            } else {
                LOG.debug(String.format("Final records report: All/Published: %s/%s.", dctmCount, publishedCount));
            }
        }

        protected List<String> getCountReport() {
            String finalStr = PropertiesHolder.getInstance().getProperty(PROP_END);
            if (StringUtils.isNotEmpty(finalStr) && Boolean.parseBoolean(finalStr)) {
                dctmCountDirect = DocumentHandler.countDocumentsUnderBooks(configs);
            }
            return ConfigReport.this.getCountReport();
        }

        protected List<String> getRecordsReport() {
            return ConfigReport.this.getRecordsReport();
        }

    }
}

package com.cchis.dctm.util.export;

import com.cchis.dctm.util.export.exception.ExportException;
import com.cchis.dctm.util.export.report.*;
import com.cchis.dctm.util.export.util.*;
import com.documentum.admin.object.*;
import com.documentum.fc.client.*;
import com.documentum.fc.client.impl.objectmanager.PersistentDataManager;
import com.documentum.fc.client.impl.session.ISession;
import com.documentum.fc.client.impl.typeddata.ITypedData;
import com.documentum.fc.common.*;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.zip.ZipOutputStream;

import static com.cchis.dctm.util.export.util.ExportConstants.*;
import static com.cchis.dctm.util.export.WebcHandler.PublishingOption.SINGLE_ITEM;

public final class WebcHandler {
    private static final Logger LOG = Logger.getLogger(WebcHandler.class);

    private WebcHandler() { }

    private static final List<String> SLOW_CONFIGS = new ArrayList<>();
    private static final List<IDfWebCacheConfig> delayedConfigs = Collections.synchronizedList(new ArrayList<>(0)); // no remove operation, don't need for CopyOnWriteArrayList

    private static final CountTimeReport COUNT_TIME_REPORT = CountTimeReport.getInstance();
    private static final IReport TOTAL_REPORT_INSTANCE = ConfigReport.getTotalReportInstance();
    private static final IReport DUPLICATE_NAMES_REPORT = DuplicateNamesReport.getInstance();
    private static final IReport MAYBE_ERROR_REPORT = MaybeErrorReport.getInstance();
    private static final BookStartDateReport BOOK_START_DATE_REPORT = BookStartDateReport.getInstance();
    private static final ReportHandler REPORT_HANDLER = ReportHandler.getInstance();

    private static final NaiveBookCache BOOK_CACHE = NaiveBookCache.getInstance();

    private static String publishTimeout;
    private static int webcLockTimeout;
    private static int jobTimeout;

    static {
        PropertiesHolder properties = PropertiesHolder.getInstance();

        publishTimeout = properties.getProperty(PROP_PUBLISH_TIMEOUT);
        webcLockTimeout = Integer.parseInt(properties.getProperty(PROP_WEBC_LOCK_TIMEOUT));
        jobTimeout = Integer.parseInt(properties.getProperty(PROP_JOB_TIMEOUT));

        // CA-INSOURCE-SRL-APPROVED CO-INSOURCE-SRL-APPROVED CO-INSOURCE-RRG-APPROVED FL-INSOURCE-SRL-APPROVED IL-INSOURCE-SRL-APPROVED LA-INSOURCE-RRR-APPROVED LA-INSOURCE-SRL-APPROVED ML-INSOURCE-GNA-APPROVED TX-INSOURCE-SIC-APPROVED TX-INSOURCE-SRL-APPROVED US-INSOURCE-RRG-APPROVED VA-INSOURCE-SRL-APPROVED WY-INSOURCE-SRL-APPROVED
        SLOW_CONFIGS.add("CA-INSOURCE-SRL-APPROVED"); // ~ 5h // after creating index time dexreases to 1.5h
        SLOW_CONFIGS.add("CO-INSOURCE-SRL-APPROVED"); // ~ 2h // after creating index time decreases to 0.5h
        SLOW_CONFIGS.add("CO-INSOURCE-RRG-APPROVED"); // < 1h, but have big number of records
        SLOW_CONFIGS.add("FL-INSOURCE-SRL-APPROVED");
        SLOW_CONFIGS.add("IL-INSOURCE-SRL-APPROVED");
        SLOW_CONFIGS.add("LA-INSOURCE-RRR-APPROVED"); // not really slow but contains lots of big-size xmls.
                                                    // Publishing in parallel using 20..30 thread ends with OOM in Documentum side
        SLOW_CONFIGS.add("LA-INSOURCE-SRL-APPROVED");
        SLOW_CONFIGS.add("ML-INSOURCE-GNA-APPROVED"); // huge number of records
        SLOW_CONFIGS.add("TX-INSOURCE-SIC-APPROVED"); // ~1h but have big number of records esp 1.0 v
        SLOW_CONFIGS.add("TX-INSOURCE-SRL-APPROVED");
        SLOW_CONFIGS.add("US-INSOURCE-RRG-APPROVED"); // ~1h but have big number of records
        SLOW_CONFIGS.add("VA-INSOURCE-SRL-APPROVED");
        SLOW_CONFIGS.add("WY-INSOURCE-SRL-APPROVED");

        REPORT_HANDLER.addReports(new IReport[] {
                COUNT_TIME_REPORT, TOTAL_REPORT_INSTANCE, DUPLICATE_NAMES_REPORT, MAYBE_ERROR_REPORT, BOOK_START_DATE_REPORT
        });

        LOG.trace(String.format("EXPORT_DIR_PATH : %s, TARGET_ROOT_DIR_PATH: %s", EXPORT_DIR_PATH, TARGET_ROOT_DIR_PATH));
        LOG.trace(String.format("publishMethod: %s, EXECUTOR_COUNT: %s, publishTimeout: %s, webcLockTimeout: %s, jobTimeout: %s, PUBLISH_LOG_LEVEL: %s",
                PublishType.getPublishMethod(), EXECUTOR_COUNT, publishTimeout, webcLockTimeout, jobTimeout, PUBLISH_LOG_LEVEL));
    }

    public enum PublishingOption {
        FULL_RECR, FULL_RECR_ASYNC, FULL, FULL_ASYNC, RECR, INCR, UPDATE, SINGLE_ITEM;

        boolean isAsync() {
            return name().contains("ASYNC");
        }
    }

    public enum WebcType {
        INSIGHT, INSOURCE, IMAGE, UNKNOWN
    }

    public enum BookCode {
        MIN, MTK, IMAGE, GNA, MBC, MGC, OAG, PRG, PST, RRG, RRR, SIC, SRL, PRC, HMR, HFC, UNKNOWN
    }

    static void republishSCSConfigsAllVersions() {
        final IDfSession session = SessionManagerHandler.getInstance().getMainSession();
        final ExecutorService executor = ExecutorsHandler.getMainExecutor();

        final Counter counter = new Counter(false);   // just to finalize correctly
        final boolean delaySlowConfigs = PublishType.delaySlowConfigs();

        try {
            final IDfEnumeration enumeration = session.getObjectsByQuery(PublishType.getDql(), DCTM_WEBC_CONFIG);
            counter.startTiming();
            final StopWatchEx directTotalTime = StopWatchEx.createStarted();
            while (enumeration.hasMoreElements()) {
                checkServices(counter);
                if (!PublishType.launchBookAsync()) {
                    launchRepublishSCSConfigAllVersions(enumeration, executor, counter);
                } else {
                    ExecutorsHandler.runBookTask(() -> launchRepublishSCSConfigAllVersions(enumeration, executor, counter));
                    ExecutorsHandler.nextBookAwait();
                }
            }
            ExecutorsHandler.waitBooksForCompletion(false);

            // Slow configs may be memory-expensive. Not sure about async publishing.
            if (delaySlowConfigs && Util.isNotEmptyCollection(delayedConfigs)) {
                setSlowTimeouts();
                ExecutorsHandler.shutdownExecutorsBeforeSlowProcessing();
                ExecutorService slowExecutor = ExecutorsHandler.getSlowExecutor();
                LOG.info("Start processing slow configs");
                PropertiesHolder.getInstance().setProperty(PROP_PROCESS_SLOW_CONFIGS, TRUE);

                delayedConfigs.forEach(config -> {
                    checkServices(counter);
                    launchRepublishSCSConfigAllVersions(config, slowExecutor, counter);
                });

                LOG.info("Slow configs are processed");
            }
            PropertiesHolder.getInstance().setProperty(PROP_END, TRUE);
            if (PublishType.launchBookAsync()) COUNT_TIME_REPORT.addTotalDirectTime(directTotalTime.stopAndGetTime());
            REPORT_HANDLER.logAndWriteReports();
        } catch (Exception e) {
            LOG.error(MSG_ERROR, e);
        } finally {
            if (counter.getCount() > 0) {
                LOG.info(String.format(MSG_PUBLISH_CONFIG_ALL_VERSIONS_SUCCESS, counter.getSuccess()));
                LOG.info(String.format(MSG_PUBLISH_CONFIG_ALL_VERSIONS_FAILURE, counter.getFailed()));
            }
            counter.stop();
            FileUtil.finalDeleteDirs();
            ExecutorsHandler.shutdownAllExecutors();
        }
    }

    private static void logExecutorsStats(String header) {
        ThreadPoolExecutor bookExec = (ThreadPoolExecutor) ExecutorsHandler.getBookExecutor();
        ThreadPoolExecutor mainExec = (ThreadPoolExecutor) ExecutorsHandler.getMainExecutor();
        LOG.info("--" + header + "--");
        LOG.info(String.format("Main Executor:%nActive Count: %s%nCompletedTaskCount: %s%nTask Count: %s%nExpected Task Count: %s",
                mainExec.getActiveCount(), mainExec.getCompletedTaskCount(),
                mainExec.getTaskCount(), ExecutorsHandler.getExpectedTaskCount()));
        LOG.info(String.format("Book Executor:%nActive Count: %s%nCompletedTaskCount: %s%nTask Count: %s",
                bookExec.getActiveCount(), bookExec.getCompletedTaskCount(),
                bookExec.getTaskCount()));
        LOG.info(String.format("%n"));
    }

    private static void setSlowTimeouts () {
        publishTimeout = PUBLISH_TIMEOUT_SLOW_CONFIG;
        webcLockTimeout = WEBC_LOCK_TIMEOUT_SLOW_CONFIG;
        jobTimeout = JOB_TIMEOUT_SLOW_CONFIG;
    }

    /**
     *
     * @param enumeration
     * @param executor
     * @param counter
     * @return false if not processed for some reason
     */
    private static boolean launchRepublishSCSConfigAllVersions (IDfEnumeration enumeration, ExecutorService executor, Counter counter) {
        String configName = EMPTY_STRING;
        try {
            if (!enumeration.hasMoreElements()) { // double-check
                return false;
            }
            IDfWebCacheConfig config = (IDfWebCacheConfig) enumeration.nextElement();
            initConfig(config); // do not forget !
            configName = config.getObjectName();
            if (PublishType.delaySlowConfigs() && SLOW_CONFIGS.contains(configName)) {
                LOG.info(String.format("Found slow config: %s. Delay processing...", configName));
                delayedConfigs.add(config);
                ExecutorsHandler.signalTaskCountSet();
                return false;
            }
            return launchRepublishSCSConfigAllVersions(config, executor, counter);
        } catch (DfException e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
        } finally {
            BOOK_CACHE.clearCache(configName);
        }
        return true;
    }

    /**
     *
     * @param config
     * @param executor
     * @param counter
     * @return false if not processed for some reason
     */
    private static boolean launchRepublishSCSConfigAllVersions(IDfWebCacheConfig config, ExecutorService executor, Counter counter) {
        Set<String> versions = DocumentHandler.getAllNumericVersionsUnderBook(config);
        ExecutorsHandler.addExpectedTaskCount(versions.size()); // should be set even for empty versions
        ConfigId cId = ConfigId.getConfigIdFailSafe(config);
        if (Util.isEmptyCollection(versions)) {
            LOG.debug("Empty versions for " + cId);
            return false;
        }
        counter.incrementStarted();
        try {
            republishSCSConfigAllVersionsInternal(config, executor, versions);
            counter.incrementSuccess();
        } catch (Exception e) {
            counter.incrementFailed();
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
        } finally {
            COUNT_TIME_REPORT.addTime(cId.getConfigName(), counter);
        }
        return true;
    }

    private static void checkServices(Counter counter) {
        if (PublishType.shouldCheckServices()) {
            ProcessHandler.checkServices(counter.getCount());
            COUNT_TIME_REPORT.addCheckServicesTime(counter);
        }
    }

    private static IDfWebCacheConfig getConfigById(IDfSession session, String configId) {
        IDfWebCacheConfig config;
        try {
            config = (IDfWebCacheConfig) session.getObject(new DfId(configId));
            initConfig(config);
            return config;
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, Level.WARN, String.format(MSG_ERROR_GET_CONFIG, configId), e);
            return null;
        }
    }

    public static IDfWebCacheConfig getConfigByPartName(IDfSession session, String partName) {
        IDfWebCacheConfig config;
        try {
            String qual = String.format("dm_webc_config where object_name like '%s%%' enable (return_top 1)", partName);
            config = (IDfWebCacheConfig)session.getObjectByQualification(qual);
            initConfig(config);
            return config;
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, Level.WARN, String.format(MSG_ERROR_GET_CONFIG, partName), e);
            return null;
        }
    }

    static void initConfig(IDfWebCacheConfig config) throws DfException {
        Objects.requireNonNull(config);
        config.initialize(config.getObjectSession(), config.getObjectId());
        if (config.isCheckedOut()) {
            LOG.debug(String.format(MSG_LOCK_OWNER, config.getLockOwner()));
            config.cancelCheckout();
        }
    }

    private static List<File> getConfigExportSets(String configId) {
        return getConfigExportSets(configId, Arrays.asList(IDS_SOURCE_PATH, SCS_TARGET_PATH));
    }


    public static List<File> getConfigExportSets(String configId, List<String> pathsList) {
        final String partConfigId = configId.substring(8);
        List<File> exportSetDirs = new ArrayList<>();

        int tryCount = 0;   // 'fast' job issue
        int maxTries = 5;
        while (true) {
            for(String path: pathsList) {
                File pathFile = new File(path);
                if (pathFile.exists()) {
                    File[] dirs = pathFile.listFiles(pathname ->
                        pathname.isDirectory() &&
                                pathname.getName().startsWith(EXPORT_SET_FOLDER_PREFIX + partConfigId)
                    );
                    if (dirs != null && dirs.length != 0) {
                        LOG.trace(String.format(MSG_EXPORT_SET_DIRS, configId, Arrays.asList(dirs).toString()));
                        exportSetDirs.addAll(Arrays.asList(dirs));
                    }
                } else {
                    LOG.warn(String.format(MSG_SCS_FOLDER_NO_ACCESS, pathFile));
                }
            }
            if (Util.isNotEmptyCollection(exportSetDirs)) {
                LOG.trace(String.format(MSG_EXPORT_SET_LENGTH, configId, exportSetDirs.size()));
                return exportSetDirs;
            } else {
                Util.sleepSilently(DEFAULT_INTERVAL);
                if(++tryCount == maxTries) {
                    throw new ExportException(String.format(MSG_ERROR_EXPORT_SET_DIRS_NOT_FOUND, configId, STR_EMPTY_STRING));
                }
            }
        }
    }

    /**
     * It is not always safe to get version from config object
     * @param session
     * @param result
     * @param version
     * @return
     */
    private static ConfigReport zipConfigExportSet(IDfSession session, PublishResult result, String version) {
        IDfWebCacheConfig config = result.getConfig();
        ConfigId cId = result.getcId();
        String configId = cId.getConfigId();
        int count;
        try {
            List<File> exportSets = getConfigExportSets(configId);
            if (exportSets.isEmpty()) {
                throw new ExportException(String.format(MSG_EXPORT_SET_ERROR, cId));
            }

            File exportSet = FileUtil.getValidExportSet(exportSets);
            ConfigReport configReport = null;
            if (result.isSingleBatch()) {
                count = zipExportSet(exportSet, result, version);
                configReport = new ConfigReport(session, config, Collections.singletonList(exportSet), count);
            } else {
                count = zipExportSet(exportSet, result, version, result.getBatch());
                FileUtils.copyDirectoryToDirectory(exportSet, BATCH_TEMP_DIR);
                File backedExportSet = new File(BATCH_TEMP_DIR + File.separator + exportSet.getName());
                PublishResult.addBatch(new PublishResult.BatchId(backedExportSet, count));
                if (result.isLastBatch()) {
                    configReport = new ConfigReport(session, config, PublishResult.getBatchDirs(), PublishResult.getTotalCount());
                    PublishResult.clearBatches();
                }
            }
            deleteExportSets(exportSets, cId);
            return configReport;
        } catch (Exception e) {
            LOG.error("Error in zipConfigExportSet()");
            throw new ExportException(e.getMessage(), e);
        }
    }

    public static File getZipFolder(String configName) {
        String subfolder = getJurisdiction(configName);
        File zipFolder = new File(EXPORT_DIR_PATH + File.separator + subfolder);
        FileUtil.mkdirs(zipFolder);
        return zipFolder;
    }

    private static int zipExportSet(File exportSet, PublishResult result, String version, int batchNo) throws DfException, IOException {
        StopWatchEx stopWatch = StopWatchEx.createStarted();
        String fullZipFileName = createZipFileName(result, version, batchNo) + ZIP_EXT;
        File zipFolder = getZipFolder(result.getConfigName());
        File zipFile = new File(zipFolder, fullZipFileName);
        try (ZipOutputStream out = new ZipOutputStream(new FileOutputStream(zipFile))) {
            return FileUtil.zipRootDirectory(exportSet, out);
        } finally {
            LOG.debug(String.format(MSG_ZIP_CREATED, fullZipFileName));
            LOG.trace("zipExportSet() time taken: " + stopWatch.stopAndGetTime());
        }
    }

    private static int zipExportSet(File exportSet, PublishResult result, String version) throws DfException, IOException {
        return zipExportSet(exportSet, result, version, 0);
    }

    public static void deleteExportSets(List<File> exportSets, ConfigId cId) {
        boolean result = true;
        for (File f: exportSets) {
            result = result && FileUtil.deleteDirectory(f);
        }
        if(result) {
            LOG.trace(String.format(MSG_DELETE_EXPORT_SET_SUCCESS, cId));
        } else {
            LOG.warn(String.format(MSG_ERROR_DELETE_EXPORT_SET, cId, exportSets));
        }
    }

    private static void clearSCSLogsForParentConfig(IDfWebCacheConfig config, List<PublishResult> errors) {
        String parentConfigName = ConfigId.getConfigIdFailSafe(config).getConfigName();
        String configNamePrefix = (PublishType.shouldCreateNewConfig() ? TEMP_PREFIX : EMPTY_STRING)
                + StringUtils.substringBeforeLast(parentConfigName, DASH);
        StringBuilder builder = new StringBuilder(String.format(DQL_SYSADMIN_SCS_LOGS_FOR_NAME, configNamePrefix));
        for (PublishResult r: errors) {
            builder.append(String.format(DQL_SYSADMIN_SCS_LOGS_EXCLUDE, r.getConfigName()));
        }
        String dql = builder.toString();
        // Do not use config session. Concurrency issue.
        DocumentHandler.destroySysObjects(SessionManagerHandler.getInstance().getCleanupSession(), dql);
    }

    @SuppressWarnings("unchecked")
    static void republishSCSConfigAllVersionsInternal(IDfWebCacheConfig config, ExecutorService executor, Set<String> versions) throws DfException {

        final ConfigId cId = ConfigId.getConfigId(config);
        final List<PublishResult> subconfigs = Collections.synchronizedList(new ArrayList<>());
        final List<CompletableFuture<List<PublishResult>>> futures = Collections.synchronizedList(new ArrayList<>());
        final List<PublishResult> errors = Collections.synchronizedList(new ArrayList<>());
        final List<PublishResult> maybeErrors = Collections.synchronizedList(new ArrayList<>());

        final ConfigReport configReport = new ConfigReport(config);

        try {
            BOOK_START_DATE_REPORT.setStartDate(cId.getConfigName());
            //LOG.info("Start publishing " + cId.getConfigName());
            LOG.debug(String.format(MSG_CONFIG_ALL_DOCUMENTS_VERSIONS, cId, versions.size(), versions));
            LOG.debug("Number of active sessions: " + SessionManagerHandler.getInstance().getSessionsCount());
            for (final String version : versions) {
                StopWatchEx stopWatch = StopWatchEx.createStarted();

                /*                final IDfWebCacheConfig childConfig = PublishType.shouldCreateNewConfig() ?
                        createWebcWithExistingConfigEx(config, version)
                        : adjustWebc(config, version);*/

                final IDfWebCacheConfig childConfig;
                final IDfSession childConfigSession;
                if (PublishType.shouldCreateNewConfig()) {
                    childConfigSession = SessionManagerHandler.getInstance().newSession();
                    childConfig = createWebcWithExistingConfigEx(childConfigSession, config, version);
                } else {
                    childConfigSession = null;
                    childConfig = adjustWebc(config, version);
                }


                LOG.trace("create/adjust config time taken: " + stopWatch.stopAndGetTime());
                List<List<String>> batcehesIds = DocumentHandler.getBatchedModifiedRecordsIdsUnderBookWithVersion(config, version, getBatchCapacity());

                if (PublishType.prefLaunchAsync()) {
                    Supplier<List<PublishResult>> supplier = () -> republishSCSConfigWithVersionAllBatches(childConfig, configReport, subconfigs, batcehesIds);
                    Consumer<List<PublishResult>> consumer = results -> {
                        postProcessPublishResult(childConfigSession, results, errors, maybeErrors, cId);
                        ExecutorsHandler.checkNextBookGo();
                    };
                    launchRepublishSCSConfigWithVersionAllBatchesAsync(futures, executor, supplier, consumer);
                    if (ExecutorsHandler.isSlowExecutor(executor)) Util.sleepSilently(TIME_CREATE_SLOW_CONFIG_INTERVAL);
                } else {
                    List<PublishResult> batchesResults = republishSCSConfigWithVersionAllBatches(childConfig, configReport, subconfigs, batcehesIds);
                    postProcessPublishResult(childConfigSession, batchesResults, errors, maybeErrors, cId);
                }
            }

            ExecutorsHandler.waitCompletionAndClear(futures);
            configReport.setErrorConfigId(PublishResult.getcIds(errors));
            configReport.setMaybeErrorConfigId(PublishResult.getcIds(maybeErrors));
            if (!errors.isEmpty()) {
                LOG.warn("errors is not empty");
                throw new ExportException(String.format(MSG_ERROR_PUBLISH_SUBCONFIG,
                        errors, cId));
            }
            LOG.info(String.format(MSG_CONFIG_PUBLISHED, cId.getConfigName()));
        } catch(Exception e) {
            LogHandler.logWithDetails(LOG, cId.getConfigName(), e);
            throw new ExportException(e.getMessage());
        } finally {
            SessionManagerHandler.getInstance().flushMainSession();
            REPORT_HANDLER.processAndWriteReport(configReport);
            LOG.debug(String.format("Records report for %s: %s", cId, configReport.getReport()));
            cleanDirs(PublishResult.getConfigIds(subconfigs));
            clearSCSLogsAsync(config, ListUtils.union(errors, maybeErrors));
            if (PublishType.shouldCreateNewConfig()) {
                cleanDctmObjectsAsync(PublishResult.getConfigIdStatusMap(subconfigs));
                destroyTempAsync(cId.getConfigName());
            } else {
                resetWebc(config);
            }
        }
    }

    private static int getBatchCapacity() {
        return USE_PUBLISHING ? MAX_SINGLE_ITEMS : Integer.MAX_VALUE;
    }

    private static void clearSCSLogsAsync(final IDfWebCacheConfig config, final List<PublishResult> results) {
        ExecutorsHandler.runCleanupTask(() -> clearSCSLogsForParentConfig(config, results));
    }

    private static void cleanDctmObjectsAsync(final Map<String, String> configIdStatusMap) {
        ExecutorsHandler.runCleanupTask(() -> cleanDctmObjects(configIdStatusMap));
   }

    private static void destroyTempAsync(final String parentConfigName) {
        ExecutorsHandler.runCleanupTask(() -> destroyTemp(parentConfigName));
    }

    private static void postProcessPublishResult(IDfSession childConfigSession, List<PublishResult> batchesResults,
                                                 List<PublishResult> errors, List<PublishResult> maybeErrors, ConfigId parentCid) {
        SessionManagerHandler.getInstance().releaseSession(childConfigSession);
        batchesResults.forEach(result -> {
            if (result.isError()) {
                errors.add(result);
            }
            if (result.isMaybeError()) {
                maybeErrors.add(result);
            }
            String logMessage = "postProcessPublishResult():" + MSG_PUBLISH_SUBCONFIG;
            LOG.debug(String.format(logMessage, result.getcId(), parentCid));
        });
    }

    private static void launchRepublishSCSConfigWithVersionAllBatchesAsync(List<CompletableFuture<List<PublishResult>>> futures, final ExecutorService executor,
                                                                           final Supplier<List<PublishResult>> s, final Consumer<List<PublishResult>> c) {
        if (futures != null && executor != null) {
            ExecutorsHandler.runTask(executor, futures, s, c);
        }
    }

    static List<PublishResult> republishSCSConfigWithVersionAllBatches (IDfWebCacheConfig childConfig, ConfigReport configReport,
                                                                       List<PublishResult> subconfigs, List<List<String>> batcehesIds) {
        List<PublishResult> results = new ArrayList<>();
        int batchesSize = batcehesIds.size();
        for (int batch = 0; batch < batchesSize; batch++) {
            final PublishResult result = new PublishResult(childConfig, subconfigs, batch, batchesSize);
            final List<String> batchIds = batcehesIds.get(batch);
            results.add(republishSCSConfigWithVersion(result, configReport, batchIds));
        }
        return results;
    }



    static PublishResult republishSCSConfigWithVersion(PublishResult result, ConfigReport configReport, List<String> idsToPublish) {
        final IDfWebCacheConfig childConfig = result.getConfig();
        ConfigId cId = ConfigId.getConfigIdFailSafe(childConfig);

        int tryCount = 0;
        int maxTries = RETRY_IF_ERROR ? 2 : 1;

        ConfigReport subconfigReport = null;

        while (true) {
            IDfSession publishSession = PublishType.isSingleSession()
                    ? SessionManagerHandler.getInstance().getMainSession()
                    //: SessionManagerHandler.getInstance().newSession();
                    : childConfig.getSession();
            LOG.trace(String.format("Thread: %s, Session: %s, Global Session: %s",
                    Thread.currentThread().getId(), publishSession.hashCode(), SessionManagerHandler.getInstance().getMainSession().hashCode()));
            try {
                String configVersion = cId.getVersion();
                StopWatchEx stopWatch = StopWatchEx.createStarted();
                switch (PublishType.getPublishMethod()) {
                    case 1: republishSCSConfigAsync(publishSession, result); break;
                    case 2: republishSCSConfigSync(publishSession, result); break;
                    case 3: republishSCSConfigFullRecrAsync(publishSession, result); break;
                    case 4: republishSCSConfigWithJob(publishSession, result); break;
                    case 5: republishSCSConfigSingleItem(publishSession, result, idsToPublish); break;
                    case 6: exportSCSConfig(result, idsToPublish); break;
                    default: throw new IllegalArgumentException("Unknown publish method");
                }
                LOG.trace(String.format("Publish time for %s: %d ms", result.getcId(), stopWatch.stopAndGetTime()));

                if (!result.isError()) {
                    subconfigReport = zipConfigExportSet(publishSession, result, configVersion);
                    if (subconfigReport == null || subconfigReport.isExportSetValid()) {
                        LOG.trace("processReport() " + cId);
                        configReport.processReport(subconfigReport);
                        return result;
                    } else {
                        throw new ExportException(String.format("Invalid export set for %s", result.getcId()));
                    }
                } else {
                    throw new ExportException(String.format("Error in result of %s.", result.getcId()));
                }
            } catch (Exception e) {
                result.preserveResponce();
                if (++tryCount == maxTries) {
                    String errorMessage = String.format("ConfigReport of %s is not valid after %d tries", cId, maxTries);
                    LOG.warn(errorMessage);
                    result.setError(new ExportException(errorMessage));
                    configReport.processReport(subconfigReport);
                    return result;
                } else {
                    LogHandler.logWithDetails(LOG, Level.WARN, Level.DEBUG,
                            String.format(MSG_WARN_PUBLISH_SUBCONFIG, result.getcId()), "Trying to republish...", e);
                    IDfWebCacheConfig config = result.getConfig();
                    try {
                        checkWebcLocked(publishSession, config);
                        if (config.isCheckedOut()) {
                            config.cancelCheckout();
                        }
                    } catch (Exception ignore) { }
                }
            }
        }
    }

    private static void cleanDctmObjects(Map<String, String> configIdStatusMap) {
        IDfSession cleanupSession = SessionManagerHandler.getInstance().getCleanupSession();
        destroyWebcs(cleanupSession, configIdStatusMap);
        destroyRegTables(cleanupSession, configIdStatusMap.keySet());
        cleanLockTable(configIdStatusMap.keySet());
    }

    private static void destroyWebcs(final IDfSession session, final Map<String, String> configIdStatusMap) {
        final Counter counter = new Counter(false);
        configIdStatusMap.forEach((configId, status) -> {
            if (destroyWebc(session, configId, status)) {
                counter.incrementSuccess();
            } else {
                counter.incrementFailed();
            }
        });
        LOG.debug(String.format("Config destroying. Success: %d. Failed: %d", counter.getSuccess(), counter.getFailed()));
    }

    private static boolean destroyWebc(IDfSession session, String configId, String resultStatus) {
        // having issue when job is still locked
        int tryCount = 0;
        int maxTries = 3;
        IDfWebCacheConfig config;
        while (true) {
            try {
                config = getConfigById(session, configId);
                String status = config.getPublishStatus();

                LOG.log((StringUtils.isEmpty(status) || !status.contains(resultStatus) ? Level.DEBUG : Level.TRACE),
                        String.format(MSG_DESTROY_CONFIG, ConfigId.getConfigId(config), config.getPublishStatus()));
                //LOG.debug(String.format(MSG_DESTROY_CONFIG, ConfigId.getConfigId(config), config.getPublishStatus()));
                destroyWebcFailSafe(config);
                //config.destroy();
                return true;
            } catch (Exception e) {
                LOG.debug(String.format("Error deleting webc: %s. Another try...", e.getMessage()));
                Util.sleepSilently(TIME_TO_DESTROY_WEBC);
                if(++tryCount == maxTries) {
                    LogHandler.logWithDetails(LOG, MSG_ERROR, e); // do not rethrow exception
                    return false;
                }
            }
        }
    }

    public static void destroyStaleRegAndUnregTables() {
        IDfSession session = SessionManagerHandler.getInstance().getMainSession();
        Connection conn = SqlConnector.getInstance().getSqlConnection();
        List<String> regTableIds = new ArrayList<>();
        Statement statement = null;
        int success = 0, failed = 0;
        try {
            statement = conn.createStatement();
            String regQuery = "select r_object_id from dm_registered_sp where object_name like 'dm_webc_8%' and SUBSTRING(object_name, 1, 16) not in (select distinct 'dm_webc_' + substring(r_object_id, 9, 16) from dm_webc_config_sp )";
            try (ResultSet rs0 = statement.executeQuery(regQuery)) {
                while (rs0.next()) {
                    String id  = rs0.getString(1);
                    regTableIds.add(id);
                }
            }
            for (String id: regTableIds) {
                try {
                    IDfSysObject regTable = (IDfSysObject) session.getObject(new DfId(id));
                    regTable.destroy();
                    success++;
                } catch (Exception e) {
                    failed++;
                    LOG.warn(MSG_ERROR, e);
                }
            }
            if (success + failed > 0) {
                LOG.debug(String.format(MSG_DESTROY_REG_TABLE, success, failed));
            }
            List<String> unregTableNames = new ArrayList<>();
            String unregQuery = "select TABLE_NAME from DM_ICSPipelineProd_docbase.INFORMATION_SCHEMA.TABLES where TABLE_NAME like 'dm_webc_8%' and substring(TABLE_NAME, 1, 16) not in (select distinct substring(r.object_name, 1,16) from dm_registered_sp r where object_name like 'dm_webc_8%')";
            try (ResultSet rs1 = statement.executeQuery(unregQuery)) {
                while (rs1.next()) {
                    String objectName  = rs1.getString(1);
                    unregTableNames.add(objectName.substring(0,16));
                }
            }

            for(String name: unregTableNames) {
                statement.addBatch(String.format("IF OBJECT_ID('%s_s', 'U') IS NOT NULL DROP TABLE %s_s", name, name));
                statement.addBatch(String.format("IF OBJECT_ID('%s_r', 'U') IS NOT NULL DROP TABLE %s_r", name, name));
                statement.addBatch(String.format("IF OBJECT_ID('%s_m', 'U') IS NOT NULL DROP TABLE %s_m", name, name));
            }
            SqlConnector.executeBatch(statement, MSG_DROP_UNREG_TABLE_SUCCESS);
        } catch (Exception e) {
            LOG.warn("Cannot destroy stale reg and unreg tables. ", e);
        } finally {
             SqlConnector.closeSilently(statement);
        }


    }

    private static void destroyRegTables(IDfSession session, Collection<String> configIds) {
        if (configIds.isEmpty()) {
            return;
        }
        List<String> ids = new ArrayList<>(configIds);
        StringBuilder builder = new StringBuilder(DQL_GET_WEBC_REG_TABLES_PREFIX);
        for (int i = 0; i < ids.size(); i++) {
            if (i != 0) {
                builder.append(OR);
            }
            String partConfigId = ids.get(i).substring(8);
            String regName = "dm_webc_" + partConfigId;
            builder.append(String.format(DQL_GET_WEBC_REG_TABLES_NAME_PATTERN, regName));
        }
        builder.append(DQL_GET_WEBC_REG_TABLES_POSTFIX);
        String dql = builder.toString();
        DocumentHandler.destroySysObjects(session, dql, MSG_DESTROY_REG_TABLE);
        dropUnregisteredTables(configIds);
    }

    private static String formatVersion(String version, String separator) {
        StringBuilder builder = new StringBuilder();
        String[] v = version.split(DOT_REGEX);
        for (int i = 0; i < v.length; i++) {
            builder.append(String.format("%03d", Integer.parseInt(v[i])));
            if (i != v.length - 1) {
                builder.append(separator);
            }
        }
        return builder.toString();
    }

    /**
     * 3 reasons why new config is created instead of reusing existing one:
     * <ol>
     *     <li>Nasty OutOfMemory  exception on Target for big configs. This is not a problem during exporting, but after that incremental publishing will not work (init. req.: Web publisher might be in use some time after exporting documents);</li>
     *     <li>Publishing process can be run asynchronously;</li>
     *     <li>New config is more lightweight (authentication is disabled and Effective Label is removed)</li>
     * </ol>
     * @param config
     * @param version
     * @return
     */
    static IDfWebCacheConfig createWebcWithExistingConfig(IDfWebCacheConfig config, String version) {
        // ! publishSession is not good for getting newConfig object
        //       IDfSession session = SessionManagerHandler.getInstance().newSession();
        IDfSession session = SessionManagerHandler.getInstance().getMainSession();
        try {
            ConfigId parentCId = ConfigId.getConfigId(config);
            LOG.trace(String.format(MSG_COPY_CONFIG, parentCId));
            String postfix = formatVersion(version, EMPTY_STRING);
            String temp = TEMP + postfix;
            String configName = createNameForConfigWithVersion(parentCId.getConfigName(), version);
            String targetRootDir = TARGET_ROOT_DIR_PATH + File.separator + temp;

            IDfId parentSourceFolderId = config.getSourceFolderID();

            IDfTime initialPublishDate = config.getInitialPublishDate();
            IDfTime lastRefreshDate = config.getRefreshDate();

            IDfId newConfigId = config.saveAsNew();
            IDfWebCacheConfig newConfig = (IDfWebCacheConfig) session.getObject(newConfigId);
            newConfig.setTitle(EMPTY_STRING);
            newConfig.setSystemValidate(false);
            newConfig.setObjectName(configName);
            newConfig.setSourceFolderID(parentSourceFolderId);
            newConfig.setInitialPublishDate(initialPublishDate);
            newConfig.setRefreshDate(lastRefreshDate);

            IDfList verLabels = newConfig.getConfigVersionLabels();
            verLabels.set(0, version);
            newConfig.setConfigVersionLabels(verLabels);

            //IDfList formats = newConfig.getSourceFormats();
            //String format = (String) formats.get(0); // produces wrong results for some image configs.
            String format = getPublishFormat(configName);
            IDfList newFormats = new DfList();
            newFormats.append(format);
            newConfig.setSourceFormats(newFormats);

            newConfig.setEffectiveLabel(null);  // don't believe we use this functionality, latest a_effective_date is 2009-02-25
                                                // and no docs with a_effective_label or a_expiration_date

            appendAdditionalAttributes(newConfig);

//                IDfId newTargetId = target.saveAsNew(false);
            IDfWebCacheTarget newTarget = newConfig.getWebCacheTarget(0);
            newTarget.setObjectName(configName);
            newTarget.setTargetRootDir(targetRootDir);
            //newTarget.setTargetVirtualDir(targetUrlPrefix); // don't believe we use this
            newTarget.setTargetVirtualDir(null);
            newTarget.setPropDBTablename(temp);

            // I switched off USER validation but need to add
            // the following two lines due to  sporadic exception during publishing: 'Error in reading target PASSWORD from target object: Invalid length'
            newTarget.setTransferUser(USER);
            newTarget.setString("transfer_user_password", PASSWORD); // setTransferUserPassword() not work

            if (TARGET_SYNC_DISABLED) {
                newTarget.setPreSyncScript(PRE_SYNC_SCRIPT);
            }

//                newTarget.save();
            newConfig.save();
            LOG.debug(String.format(MSG_CREATED_SUBCONFIG, ConfigId.getConfigId(newConfig), parentCId));

            return newConfig;
        } catch (Exception e) {
            LOG.error("Error in createWebcWithExistingConfig()");
            throw new ExportException(e.getMessage(), e);
        } /*finally {
            SessionManagerHandler.getInstance().releaseSession(session);
        }*/
    }

    public static IDfWebCacheConfig createWebcWithExistingConfigEx(IDfSession configSession, IDfWebCacheConfig config, String version) {
        try {
            ConfigId parentCId = ConfigId.getConfigId(config);
            LOG.trace(String.format(MSG_COPY_CONFIG, parentCId));
            String postfix = formatVersion(version, EMPTY_STRING);
            String temp = createTempSqlName(parentCId.getConfigName()) + postfix;

            String configName = createNameForConfigWithVersion(parentCId.getConfigName(), version);
            String targetRootDir = TARGET_ROOT_DIR_PATH + File.separator + temp;

            IDfId parentSourceFolderId = config.getSourceFolderID();

            IDfTime initialPublishDate = config.getInitialPublishDate();
            IDfTime lastRefreshDate = config.getRefreshDate();

            IDfWebCacheTarget oldTarget = (IDfWebCacheTarget) configSession.getObject((IDfId)config.getTargetID().get(0));
            IDfWebCacheTarget newTarget = (IDfWebCacheTarget) configSession.getObject(oldTarget.saveAsNew(true));
            newTarget.setObjectName(configName);
            newTarget.setTargetRootDir(targetRootDir);
            newTarget.setTargetVirtualDir(null);
            newTarget.setPropDBTablename(temp);

           newTarget.setTransferUser(USER);
            newTarget.setString("transfer_user_password", PASSWORD); // setTransferUserPassword() not work
            if (TARGET_SYNC_DISABLED) {
                newTarget.setPreSyncScript(PRE_SYNC_SCRIPT);
            }
            newTarget.setString("last_publish_dir", null);
            newTarget.setString("replication_status", null);
            newTarget.save();


//            IDfWebCacheConfig newConfig = (IDfWebCacheConfig) configSession.newObject("dm_webc_config"); // doesn't work correctly for some reason
            DfWebCacheConfig newConfigOrig = new DfWebCacheConfig();
            newConfigOrig.setObjectSession((ISession)configSession);
            PersistentDataManager dataManager = ((ISession) configSession).getDataManager();
            ITypedData data = dataManager.getData(config.getObjectId(), new DfGetObjectOptions(), false, false);
            newConfigOrig.setData(data);
            IDfWebCacheConfig newConfig = (IDfWebCacheConfig) configSession.getObject(newConfigOrig.saveAsNew());
            newConfig.removeWebCacheTarget(0);  // important! Old target infiltrates to new config through data object.

            newConfig.setTransferMethod("internal");
            newConfig.setNotificationUser(USER);

            newConfig.setTitle(EMPTY_STRING);
            newConfig.setSystemValidate(false);
            newConfig.setObjectName(configName);
            newConfig.setSourceFolderID(parentSourceFolderId);
            newConfig.setContentlessProperties(true);
            newConfig.setFolderProperties(true);
            newConfig.setPublishStatus(EMPTY_STRING);
            newConfig.setInitialPublishDate(initialPublishDate);
            newConfig.setRefreshDate(lastRefreshDate);
            newConfig.setEventNumber(0);
            newConfig.setLastIncrementCount(0);

            IDfList verLabels = newConfig.getConfigVersionLabels();
            verLabels.set(0, version);
            newConfig.setConfigVersionLabels(verLabels);

            String format = getPublishFormat(configName);
            IDfList newFormats = new DfList();
            newFormats.append(format);
            newConfig.setSourceFormats(newFormats);

            newConfig.setEffectiveLabel(null);

            newConfig.addWebCacheTarget(newTarget); // should be before appendAttributes

            if (SET_SCS_EXTRA_ARGS) {       // no need to set if source_attrs_only = false;
                appendAdditionalAttributes(newConfig);
            }

            newConfig.save();
            LOG.debug(String.format(MSG_CREATED_SUBCONFIG, ConfigId.getConfigId(newConfig), parentCId));

            return newConfig;
        } catch (Exception e) {
            LOG.error("Error in createWebcWithExistingConfig()");
            throw new ExportException(e.getMessage(), e);
        }
    }


    private static void appendAdditionalAttributes(IDfWebCacheConfig config) {
        appendWebcAttr(config, new String[] {ATTR_R_VERSION_LABEL, STRING_TYPE, THIRTY_TWO, FALSE_SYM});
        appendWebcAttr(config, new String[] {ATTR_R_OBJECT_TYPE, STRING_TYPE, THIRTY_TWO, FALSE_SYM});
        appendWebcAttr(config, new String[] {ATTR_R_CURRENT_STATE, INTEGER_TYPE, ZERO, FALSE_SYM});
    }

    private static void removeAdditionalAttributes(IDfWebCacheConfig config) {
        removeWebcAttr(config, ATTR_R_VERSION_LABEL);
        removeWebcAttr(config, ATTR_R_OBJECT_TYPE);
        removeWebcAttr(config, ATTR_R_CURRENT_STATE);
    }

    public static String createTempSqlName(String existingConfigName) {
        return TEMP + Util.trimEnd(existingConfigName, APPROVED_POSTFIX).replace(DASH, EMPTY_STRING);
    }

    public static String createNameForConfigWithVersion(String existingConfigName, String version, String prefix, String postfix) {
        StringBuilder builder = new StringBuilder();
        if (StringUtils.isNotEmpty(prefix)) {
            builder.append(prefix);
        }
        builder.append(StringUtils.substringBeforeLast(existingConfigName, DASH));
        if (StringUtils.isNotEmpty(version)) {
            builder.append(DASH + formatVersion(version, DOT));
        }
        if (StringUtils.isNotEmpty(postfix)) {
            builder.append(postfix);
        }
        return builder.toString();
    }

    private static String createNameForConfigWithVersion(String existingConfigName, String version) {
        return createNameForConfigWithVersion(existingConfigName, version, TEMP_PREFIX, null);
    }

    public static String getConfigNameWithoutVersion(String existingConfigName) {
        return createNameForConfigWithVersion(existingConfigName, null, null, null);
    }

    /**
     * Not always safe get version from config object.
     * @param result
     * @param version may be null
     * @return
     * @throws DfException
     */
    private static String createZipFileName(PublishResult result, String version, int batchNo) throws DfException {
        IDfWebCacheConfig config = result.getConfig();
        ConfigId cId = ConfigId.getConfigId(config);
        String ver = StringUtils.isNotEmpty(version) ? version : cId.getVersion();
        switch (PublishType.getCurrent()) {
            case FULL: return cId.getConfigName().replace(TEMP_PREFIX, EMPTY_STRING);
            case DELTA:
                String postfix = !result.isSingleBatch() ? DASH + String.valueOf(batchNo) : null;
                return createNameForConfigWithVersion(cId.getConfigName(), ver, DELTA_PREFIX, postfix);

        }
        throw new IllegalArgumentException(MSG_ERROR_UNKNOWN_PUBLISH_TYPE);
    }

    private static IDfWebCacheConfig adjustWebc(IDfWebCacheConfig config, String version) throws DfException {
        appendAdditionalAttributes(config);
        setWebcVersion(config, version);
        if (TARGET_SYNC_DISABLED) {
            IDfWebCacheTarget target = config.getWebCacheTarget(0);
            target.setPreSyncScript(PRE_SYNC_SCRIPT);
            config.save();
        }
        return config;
    }

    private static void setWebcVersion(IDfWebCacheConfig config, String version) throws DfException {
        if (config.isCheckedOut()) {
            config.cancelCheckout();
        }
        IDfList verLabels = config.getConfigVersionLabels();
        verLabels.set(0, version);
        config.setConfigVersionLabels(verLabels);
        config.save();
    }

    static void resetWebc(IDfWebCacheConfig config) throws DfException {
        removeAdditionalAttributes(config);
        setWebcVersion(config, APPROVED_LABEL);
        if (TARGET_SYNC_DISABLED) {
            IDfWebCacheTarget target = config.getWebCacheTarget(0);
            target.setPreSyncScript(EMPTY_STRING);
            config.save();
        }
    }

    private static void removeWebcAttr(IDfWebCacheConfig config, String attrName) {
        try {
            while (true) {
                IDfList attrNameAndTypeList = config.getSourceAttrs();
                int index1 = attrNameAndTypeList.findStringIndex(attrName);
                if (index1 < 0) {
                    break;
                }
                IDfList attrTypeList = config.getSourceAttrsType();
                IDfList attrLengthList = config.getSourceAttrLength();
                IDfList attrRepList = config.getSourceAttrIsRep();

                attrNameAndTypeList.remove(index1);
                attrTypeList.remove(index1);
                attrLengthList.remove(index1);
                attrRepList.remove(index1);
                config.setSourceAttrs(attrNameAndTypeList);
                config.setSourceAttrType(attrTypeList);
                config.setSourceAttrLength(attrLengthList);
                config.setSourceAttrIsRep(attrRepList);
                config.save();
            }
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
        }
    }


    public static void appendWebcAttr(IDfWebCacheConfig config, String[] attrProps) {
        try {
            ConfigId cId = ConfigId.getConfigId(config);
            IDfList attrNameAndTypeList = config.getSourceAttrs();
            int index1 = attrNameAndTypeList.findStringIndex(attrProps[0]);
            if (index1 >= 0) {
                LOG.debug(String.format("Attribute %s has already set for config %s", attrProps[0], cId));
                return;
            }
            IDfList attrTypeList = config.getSourceAttrsType();
            IDfList attrLengthList = config.getSourceAttrLength();
            IDfList attrRepList = config.getSourceAttrIsRep();

            // adding attribute
            String attrName = attrProps[0];
            String attrType = attrProps[1];
            String attrLength = attrProps[2];
            String attrIsRepeating = attrProps[3];

            attrNameAndTypeList.appendString(attrName);
            attrTypeList.appendString(attrType);
            attrLengthList.appendString(attrLength);
            attrRepList.appendString(attrIsRepeating);

            config.setSourceAttrs(attrNameAndTypeList);
            config.setSourceAttrType(attrTypeList);
            config.setSourceAttrLength(attrLengthList);
            config.setSourceAttrIsRep(attrRepList);
            config.save();
            LOG.trace(String.format(MSG_CONFIG_APPEND_ATTR, attrProps[0], cId ));

        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
            throw new ExportException(e.getMessage());
        }
    }

    static void republishSCSConfigFullRecrSync(IDfSession session, PublishResult result) {
        republishSCSConfig(session, result, null, PublishingOption.FULL_RECR);
    }

    private static void republishSCSConfigFullRecrAsync(IDfSession session, PublishResult result) {
        republishSCSConfig(session, result, null, PublishingOption.FULL_RECR_ASYNC);
    }

    // Job
    private static void republishSCSConfigWithJob(IDfSession session, PublishResult result) {
        IDfWebCacheConfig config = result.getConfig();
        try {
            updatePublishingJob(session, config);
            runPublishingJob(session, config);
            result.setOk(checkJobStatus(session, result));
            Util.sleepSilently(DEFAULT_INTERVAL);
        } catch(Exception e) {
            LogHandler.logWithDetails(LOG, "Error in republishSCSConfigWithJob()", e);
            result.setError(e);
        }
    }

    private static boolean checkJobStatus(IDfSession session, PublishResult result) throws Exception {
        ConfigId cId = result.getcId();
        long start = System.currentTimeMillis();
        String oldStatus = EMPTY_STRING;
        while (true) {
            Util.sleepSilently(STATUS_CHECK_INTERVAL);
            String status = getPublishingJobStatus(session, cId.getConfigId());
            if (!status.equalsIgnoreCase(oldStatus)) {
                oldStatus = status;
                LOG.trace(String.format(MSG_JOB_STATUS, cId, status));
            }

            long current = System.currentTimeMillis();
            if (current - start > jobTimeout * 1000 || status.startsWith("TIMED OUT")) {
                throw new ExportException(String.format(MSG_ERROR_PUBLISH_TIMEOUT, cId));
            }


            if ("COMPLETED".equals(status)) {
                // got weird problem when Job status is 'completed' but config status is not yet
                // (but will be completed successfully in a moment though).
                // At the same time checking webc status dramatically decreases performance.
                // return true;
                return checkWebcStatusFast(session, result);

            }
            if (status.startsWith("FAILED")) {
                ServerErrorCounter.getInstance().incrErrorCount();
                return false;
            }
        }
    }

    private static void updatePublishingJob(IDfSession session, IDfWebCacheConfig config) {
        try {
            String configId = config.getObjectId().getId();
            IDfJob job = (IDfJob) session.getObjectByQualification(String.format(DQL_GET_PUBLISH_JOB_BY_CONFIG_ID, configId));
            LOG.trace(String.format("Update object %s %s", job.getObjectId().getId(), job.getObjectName()));
            job.appendString("method_arguments", "-config_location D:\\Documentum\\dba\\config\\ICSPipelineProd");
            job.appendString("method_arguments", "-method_trace_level " + PUBLISH_LOG_LEVEL);
            job.appendString("method_arguments", "-full_refresh T");
            job.appendString("method_arguments", "-recreate_property_schema T");
            job.appendString("method_arguments", "-update_property_schema F");
            job.appendString("method_arguments", "-launch_async F");

            String sourceFolderPath = getPublishFolder(config);
            job.setLogEntry(sourceFolderPath);

            job.setRunMode(5);
            job.setRunInterval(1);

            String subject = job.getSubject();
            subject = subject.replaceAll(WEB_PUBLISH_JOB_SUBJECT, STR_EMPTY_STRING);
            job.setSubject(subject);

            job.save();
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
        }
    }

    private static void runPublishingJob(IDfSession session, IDfWebCacheConfig config) {
        try {
            ConfigId cId = ConfigId.getConfigId(config);
            IDfJob job = (IDfJob) session.getObjectByQualification(String.format(DQL_GET_PUBLISH_JOB_BY_CONFIG_ID, cId.getConfigId()));
            if (job != null) {
                job.setRunNow(true);
                job.setString(ATTR_A_CURRENT_STATUS, SUBMITTED);
                job.save();
                Util.sleepSilently(DEFAULT_INTERVAL);
            } else {
                LOG.error(String.format("Job not found for %s", cId));
            }
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
        }
    }

    private static String getPublishingJobStatus(IDfSession session, String configId) {
        try {
            IDfJob job = (IDfJob) session.getObjectByQualification(String.format(DQL_GET_PUBLISH_JOB_BY_CONFIG_ID, configId));
            return job.getCurrentStatus();
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
            throw new ExportException(e.getMessage());
        }
    }


    private static void prepublishSCSConfig (IDfSession session, PublishResult result) {
        IDfWebCacheConfig childConfig = result.getConfig();
        ConfigId cId = result.getcId();
        PublishResult fullPublishResult = new PublishResult(childConfig, null);

        republishSCSConfigFullRecrSync(session, fullPublishResult);

        if (fullPublishResult.isNotOk()) {
            LOG.error("prepublishSCSConfig(): Full publishing is not OK");
        }

        // should set success status. Otherwise single item publishing fails
        try {
            childConfig = getConfigById(session, cId.getConfigId()); // update is needed if single.session = false
            childConfig.setPublishStatus("Publish Event # 0 : Completed Successfully");
            childConfig.save();
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, "prepublishSCSConfig(): Cannot set Successful publish state", e);
        }

        List<File> exportSets = getConfigExportSets(cId.getConfigId());
        deleteExportSets(exportSets, cId);
    }

    public static void republishSCSConfigSingleItem (IDfSession session, PublishResult result, List<String> docIds) {
        if (PublishType.shouldCreateNewConfig()) {
            if (result.isFirstBatch()) {
                prepublishSCSConfig(session, result);
            }
            result.setWebcCompletedStatus(PublishType.WEBC_SUCCESS);
        }
        republishSCSConfig(session, result, docIds, SINGLE_ITEM);
    }

    private static void exportSCSConfig(PublishResult result, List<String> docIds) {
        IDfWebCacheConfig config = result.getConfig();
        ConfigId cId = ConfigId.getConfigIdFailSafe(config);
        try {
            Function<ReportRecord, String> nameMap = ReportRecord::getFilteredObjectNameNullSafe;
            String exportPathStr = IDS_SOURCE_PATH + File.separator + EXPORT_SET_FOLDER_PREFIX + cId.getConfigId().substring(8);
            Path exportPath = Paths.get(exportPathStr);
            FileUtil.exportFilesUnderBook(config, docIds, nameMap, exportPath);
        } catch (Exception e) {
            result.setError(e);
        }
    }

    public static void republishSCSConfig(IDfSession session, PublishResult result, List<String> docIds,
                                             PublishingOption option) {

        IDfWebCacheConfig config = result.getConfig();
        int idsSize = Util.isNotEmptyCollection(docIds) ? docIds.size() : 0;
        IDfCollection coll = null;
        String fullRefresh = FALSE_SYM;
        String updatePropSchema = FALSE_SYM;
        String recreatePropSchema = FALSE_SYM;
        String launchAsync = FALSE_SYM;

        try {
            ConfigId cId = ConfigId.getConfigId(config);
            switch (option) {
                case FULL_RECR_ASYNC:
                    fullRefresh = TRUE_SYM;
                    recreatePropSchema = TRUE_SYM;
                    launchAsync = TRUE_SYM;
                    break;

                case FULL_RECR:
                    fullRefresh = TRUE_SYM;
                    recreatePropSchema = TRUE_SYM;
                    break;

                case SINGLE_ITEM:
                    fullRefresh = FALSE_SYM;
                    recreatePropSchema = FALSE_SYM;
                    updatePropSchema = FALSE_SYM;
                    break;

                default:
                    break;
            }

            IDfList argList = new DfList();
            IDfList dataTypeList = new DfList();
            IDfList valueList = new DfList();

            argList.appendString("APP_SERVER_NAME");
            dataTypeList.appendString("S");
            valueList.appendString("WebCache");

            argList.appendString("SAVE_RESPONSE");
            dataTypeList.appendString("I");
            valueList.appendString("-1");

            argList.appendString("LAUNCH_ASYNC");
            dataTypeList.appendString("B");
            valueList.appendString(launchAsync);

            argList.appendString("TRACE_LAUNCH");
            dataTypeList.appendString("B");
            valueList.appendString(FALSE_SYM);

            argList.appendString("TIME_OUT");
            dataTypeList.appendString("I");
            valueList.appendString(publishTimeout);

            argList.appendString("ARGUMENTS");
            dataTypeList.appendString("S");

            // Build the arguments list
            String argumentslist = EMPTY_STRING;
            argumentslist = argumentslist + " -docbase_name " + session.getDocbaseName();
            argumentslist = argumentslist + " -config_object_id " + cId.getConfigId();
            argumentslist = argumentslist + " -method_trace_level " + PUBLISH_LOG_LEVEL;
            argumentslist = argumentslist + " -recreate_property_schema " + recreatePropSchema;
            argumentslist = argumentslist + " -full_refresh " + fullRefresh;
            argumentslist = argumentslist + " -update_property_schema " + updatePropSchema;

            if (SINGLE_ITEM.equals(option)) {
                if(Util.isNotEmptyCollection(docIds)) {
                    argumentslist = argumentslist + " -force_refresh T";
                    String objectIds = StringUtils.join(docIds, " ");
                    argumentslist = argumentslist + " -source_object_id \"" + objectIds + "\"";
                } else {
                    LOG.info("Publishing option is single-item, but list of Ids is not provided. Defaulted to incremental");
                }
            } else if (Util.isNotEmptyCollection(docIds)) {
                LOG.warn("List of published document is provided but option is not single-item. The list will be ignored");
            }

            LOG.debug(String.format("argument_list (%d): %s", idsSize, argumentslist));
            valueList.appendString(argumentslist);

            //apply,c,NULL,HTTP_POST,APP_SERVER_NAME,S,WebCache,SAVE_RESPONSE,I,-1,LAUNCH_A
            //SYNC,B,F,TRACE_LAUNCH,B,T,TIME_OUT,I,120,ARGUMENTS,S,-docbase_name DCTMDEV -config_object_id 08000001800dcfab -force_refresh T -method_trace_level 0 -full_refresh F -source_object_id "09000001800de01d 09000001800de01e"

            LOG.trace("Session for apply: " + session.hashCode());
            coll = session.apply("NULL", "HTTP_POST", argList, dataTypeList, valueList);
            postProcessPublishSCSConfig(session, coll, result, false);
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, "Error in republishSCSConfig()", e);
            result.setError(e);
        } finally {
            Util.closeCollection(coll);
        }
    }

    private static void republishSCSConfigSync(IDfSession session, PublishResult result) {
        publishSCSConfig(session, result, PublishingOption.FULL_RECR);
    }

    private static void republishSCSConfigAsync(IDfSession session, PublishResult result) {
        publishSCSConfig(session, result,
                TARGET_SYNC_DISABLED ? PublishingOption.FULL_ASYNC : PublishingOption.FULL_RECR_ASYNC);
    }

    public static void publishSCSConfig(IDfSession session, PublishResult result, PublishingOption option) {
        IDfCollection collectionTemp = null;
        IDfWebCacheConfig config = result.getConfig();
        ConfigId cId = result.getcId();
        try {
            LOG.trace(String.format(MSG_PUBLISH_CONFIG, cId));
            LOG.trace("Config Session: " + config.getSession().hashCode());
            switch(option) {
                case FULL_RECR_ASYNC: collectionTemp = config.publish(true, PUBLISH_LOG_LEVEL_INT, true, true, false, false); break;
                case FULL_RECR: collectionTemp = config.publish(false, PUBLISH_LOG_LEVEL_INT, true, true, false, false); break;
                case FULL: collectionTemp = config.publish(false, PUBLISH_LOG_LEVEL_INT, true, false, false, false); break;
                case FULL_ASYNC: collectionTemp = config.publish(true, PUBLISH_LOG_LEVEL_INT, true, false, false, false); break;
                case RECR: collectionTemp = config.publish(false, PUBLISH_LOG_LEVEL_INT, false, true, false, false); break;
                case INCR: collectionTemp = config.publish(false, PUBLISH_LOG_LEVEL_INT, false, false, false, false); break;
                case UPDATE: collectionTemp = config.publish(false, PUBLISH_LOG_LEVEL_INT, false, false, false, true); break;
                default:throw new IllegalArgumentException("Unknown option");
            }

            postProcessPublishSCSConfig(session, collectionTemp, result, option.isAsync());
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, "Error in republishSCSConfigWithVersion()", e);
            ServerErrorCounter.getInstance().incrErrorCount();
            result.setError(e);
        } finally {
            Util.closeCollection(collectionTemp);
        }
    }

    private static void postProcessPublishSCSConfig(IDfSession session, IDfCollection resultCollection, PublishResult result, boolean isAsync) throws DfException {
        IDfWebCacheConfig config = result.getConfig();
        ConfigId cId = result.getcId();
        Util.sleepSilently(DEFAULT_INTERVAL); // some time is needed to lock config
        resultCollection.next();
        String info = cId + " published: " + Util.joinStrings(PIPE,
                resultCollection.getValue(HTTP_RESPONSE_RESULT),
                resultCollection.getValue(HTTP_RESPONSE_STATUS),
                resultCollection.getValue(HTTP_RESPONSE_TIMED_OUT));
        LOG.debug(info);

        //if (!isAsync) result.setResponceDocId(resultCollection.getId("response_doc_id")); // both sync and async: set id on error only

        if (isAsync) Util.sleepSilently(DEFAULT_INTERVAL);
        checkWebcLocked(session, config); // try to check if publishing is completed.
        result.setOk(checkWebcStatus(session, result, isAsync));
    }

    private static void checkWebcLocked(IDfSession session, IDfWebCacheConfig config) {
        ConfigId cId = ConfigId.getConfigIdFailSafe(config);
        long start = System.currentTimeMillis();
        while(isWebcLocked(session, config)) {
            long current = System.currentTimeMillis();
            if (current - start > webcLockTimeout * 1000) {
                throw new ExportException(String.format(MSG_ERROR_PUBLISH_TIMEOUT, cId));
            }
            //LOG.trace(String.format(MSG_PUBLISH_WAIT_UNLOCK, cId));
            Util.sleepSilently(WEBC_LOCK_INTERVAL);
        }
    }

    private static boolean isWebcLocked(IDfSession session, IDfWebCacheConfig config) {
        IDfCollection holderCollection = null;
        final String holderAlias = "holder";
        ConfigId cId = ConfigId.getConfigIdFailSafe(config);
        int tryCount = 0;
        int maxTries = 16;
        try {
            int count;
            while(true) {
                try {
                    count = Util.getCount(session, String.format(DQL_GET_WEBCLOCK_COUNT_FOR_NAME, cId.getConfigId()));
                    if(count != 1) {
                        LOG.trace(String.format(MSG_WEBC_LOCK_COUNT, cId, count));
                        LOG.trace(String.format(MSG_WEBCLOCK_NOT_FOUND_ERROR_PROGRESS, cId));
                        throw new ExportException(String.format(MSG_WEBCLOCK_NOT_FOUND_ERROR_PROGRESS, cId));
                    }
                    break;
                } catch (Exception e) {
                    Util.sleepSilently(TIME_TO_LOCK);
                    if (++tryCount == maxTries)
                        throw new ExportException(String.format(MSG_WEBCLOCK_NOT_FOUND_ERROR, cId));
                }
            }

            holderCollection = Util.runQuery(session, String.format(DQL_GET_WEBCLOCK_FOR_NAME, holderAlias, cId.getConfigId()));
            holderCollection.next();
            String holder = holderCollection.getString(holderAlias);
            LOG.trace(String.format(MSG_WEBC_LOCK_COUNT_HOLDER, cId, count, holder));

            return !EMPTY_STRING.equals(holder);
        } catch (Exception e) {
            LOG.error("Error in isWebcLocked()");
            throw new ExportException(String.format(MSG_ERROR_IS_WEBC_LOCKED, cId), e);
        } finally {
            Util.closeCollection(holderCollection);
        }
    }

    private static boolean checkWebcStatus(IDfSession session, PublishResult result, boolean checkLock) throws DfException {
        IDfWebCacheConfig config = result.getConfig();
        ConfigId cId = result.getcId();
        String completedStatus = result.getWebcCompletedStatus();
        int tryCount = 0;   // 'fast' job/config issue
        int maxTries = 5;

        while (true) {
            String status = getConfigById(session, cId.getConfigId()).getPublishStatus(); // sync with database
            if (status.contains(completedStatus)) {
                LOG.trace(String.format(MSG_WEBC_STATUS, cId, status));
                return true;
            }

            Util.sleepSilently(STATUS_CHECK_INTERVAL);
            LOG.trace(String.format("Status for %s: %s != '%s'. Another try...", cId, status, completedStatus));
            if (checkLock) {
                checkWebcLocked(session, config);
            }
            if (++tryCount == maxTries) {
                LOG.warn(String.format(MSG_WEBC_STATUS, cId, status));
                ServerErrorCounter.getInstance().incrErrorCount();
                return false;
            }
        }
    }

    private static boolean checkWebcStatusFast(IDfSession session, PublishResult result) throws DfException {
        return checkWebcStatus(session, result, false);
    }

    private static void cleanDirs(List<String> ids) {
        FileUtil.cleanTargetRootDir();
        FileUtil.cleanResponseDir(ids);
    }

    private static void destroyTemp(String parentConfigName) {
        dropSqlTempObject(parentConfigName);
    }

    private static void dropSqlTempObject(String parentConfigName) {
        Connection conn = SqlConnector.getInstance().getSqlConnection();
        Statement statement = null;
        try {
            statement = conn.createStatement();
            String query = String.format(SQL_GET_TEMP_OBJECTS, SCS_DB_NAME, createTempSqlName(parentConfigName));
            Map<String, String> tempTables = new LinkedHashMap<>();
            try (ResultSet rs = statement.executeQuery(query)) {
                while (rs.next()) {
                    String tableName = rs.getString(1);
                    String tableType = rs.getString(2);
                    String objectType = "BASE TABLE".equals(tableType) ? "U" : "V"; // have two options 'BASE TABLE' and 'VIEW'
                    tempTables.put(tableName, objectType);
                }
                //LOG.trace(String.format("Dropping temp objects : %s", tempTables.keySet()));
            }
            statement.addBatch(String.format("use %s", SCS_DB_NAME));

            for (Map.Entry<String, String> entry : tempTables.entrySet()) {
                String tableName = entry.getKey();
                String objectType = entry.getValue();
                String object = "U".equals(objectType) ? "TABLE" : "VIEW";
                statement.addBatch(String.format(SQL_DROP_TEMP_OBJECTS, tableName, object, objectType));
            }
            statement.addBatch(String.format("use %s", "DM_ICSPipelineProd_docbase"));
            int success = 0, failed = 0;
            try {
                int[] results = statement.executeBatch();
                for(int j: results) {
                    if (j == Statement.EXECUTE_FAILED) {
                        LOG.debug(String.format("Error executing #%d in batch", j));
                        failed++;
                    } else {
                        success++;
                    }
                }
                success -= 2; // the first and last query in batch is always executed
            } catch (Exception e) {
                LogHandler.logWithDetails(LOG, Level.WARN, MSG_SQL_BATCH_ERROR, e);
            } finally {
                LOG.debug(String.format(MSG_DESTROY_TEMP_TABLE, success, failed));
            }
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, Level.WARN, MSG_ERROR, e);
        }
        finally {
            SqlConnector.closeSilently(statement);
        }
    }

    private static void dropUnregisteredTables(Collection<String> configIds) {
        Connection conn = SqlConnector.getInstance().getSqlConnection();
        Statement statement = null;
        try {
            statement = conn.createStatement();
            for (String id: configIds) {
                String name = id.substring(8,16);
                statement.addBatch(String.format(SQL_DROP_UNREG_TABLES_S, name));
                statement.addBatch(String.format(SQL_DROP_UNREG_TABLES_R, name));
                statement.addBatch(String.format(SQL_DROP_UNREG_TABLES_M, name));
            }
            SqlConnector.executeBatch(statement, MSG_DESTROY_UNREG_TABLE);
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, Level.WARN, MSG_ERROR, e);
        }
        finally {
            SqlConnector.closeSilently(statement);
        }
        //LOG.debug(MSG_DROP_UNREG_TABLE_SUCCESS);
    }

    private static void cleanLockTable(Collection<String> configIds) {
        Connection conn = SqlConnector.getInstance().getSqlConnection();
        Statement statement = null;
        try {
            statement = conn.createStatement();
            for (String id: configIds) {
                statement.addBatch(String.format("DELETE FROM DM_ICSPipelineProd_docbase.dbo.webc_lock_s WHERE object_name = '%s'", id));
            }
            SqlConnector.executeBatch(statement, MSG_CLEAN_LOCK_TABLE);
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, Level.WARN, MSG_ERROR, e);
        }
        finally {
            SqlConnector.closeSilently(statement);
        }
    }

    public static WebcType getWebcType(String configName) {
        WebcType currentType = null;
        if (configName.contains("IMAGES")) {
            currentType = WebcType.IMAGE;
        } else if (configName.contains("INSIGHT")) {
            currentType = WebcType.INSIGHT;
        } else if (configName.contains("INSOURCE")) {
            currentType = WebcType.INSOURCE; // do not include images
        }
        if (currentType == null) {
            currentType = WebcType.UNKNOWN;
        }
        return currentType;
    }

    public static BookCode getBookCode(String configName) {
        switch (getWebcType(configName)) {
            case IMAGE: return BookCode.IMAGE;
            case INSIGHT:
                if (configName.contains("BILL")) return BookCode.MIN;
                if (configName.contains("REGS")) return BookCode.MTK;
                break;
            case INSOURCE:  return BookCode.valueOf(configName.substring(12, 15));
            default: return BookCode.UNKNOWN;
        }
        return BookCode.UNKNOWN;
    }

    public static String getPublishType(IDfWebCacheConfig config) {
        return DCTM_DM_DOCUMENT; // currently real object type is not considered
    }

    public static String getPublishFormat(String configName) {
        switch (getWebcType(configName)) {
            case IMAGE: return "gif";
            case INSOURCE: return "xml";
            case INSIGHT: return "html";
            default: throw new IllegalArgumentException(MSG_ERROR_UNKNOWN_WEBC_TYPE);
        }
    }

    public static String getPublishFolder(IDfWebCacheConfig config, boolean useCache) throws DfException {
        return getPublishFolder(config.getSession(), config, useCache);
    }

    public static String getPublishFolder(IDfWebCacheConfig config) throws DfException {
        return getPublishFolder(config, USE_BOOK_CACHE);
    }

    public static String getPublishFolder(IDfSession session, IDfWebCacheConfig config) throws DfException {
        return getPublishFolder(session, config, USE_BOOK_CACHE);
    }


    public static String getPublishFolder(IDfSession session, IDfWebCacheConfig config, boolean useCache) throws DfException {
        String configName = EMPTY_STRING;
        if (useCache) {
            configName = ConfigId.getConfigIdFailSafe(config).getConfigName();
            String cacheValue = (String) BOOK_CACHE.get(configName, CACHE_PUBLISH_FOLDER);
            if (StringUtils.isNotEmpty(cacheValue)) {
                return cacheValue;
            }
        }
        IDfId folderId = config.getSourceFolderID();
        IDfFolder folder = (IDfFolder) session.getObject(folderId);
        String currentPublishFolder = folder.getFolderPath(0);
        if (useCache) {
            BOOK_CACHE.put(configName, CACHE_PUBLISH_FOLDER, currentPublishFolder);
        }
        return currentPublishFolder;
    }

    private static IDfJob createPublishJob(IDfWebCacheConfig config) throws DfException {
        IDfSession session = SessionManagerHandler.getInstance().getMainSession();
        String configId = config.getObjectId().getId();
        String subject = config.getObjectName();
        String logEntry = config.getLogEntry();
        IDfJob job = (IDfJob) session.newObject("dm_job");
        job.setObjectName("dm_WebPublish_" + configId);
        job.setTitle("Web Publishing");
        job.setSubject(subject);
        job.setLogEntry(logEntry);
        job.setString("method_name", "dm_webcache_publish");
        job.setBoolean("pass_standard_arguments", false);
        job.setBoolean("is_inactive", true);
        job.appendString("method_arguments", "-config_object_id " + configId);
        job.appendString("method_arguments", "-docbase_name ICSPipelineProd");
        job.appendString("method_arguments", "-config_location D:\\Documentum\\dba\\config\\ICSPipelineProd");
        job.appendString("method_arguments", "-launch_async T");
        job.appendString("method_arguments", "-full_refresh F");
        job.appendString("method_arguments", "-recreate_property_schema F");
        job.appendString("method_arguments", "-update_property_schema F");
        job.appendString("method_arguments", "-method_trace_level 0");
        IDfTime time = new DfTime();
        job.setStartDate(time);
        job.setNextInvocation(time);
        job.link("/System/Sysadmin/WebCache");
        job.save();
        return job;
    }

    public static void destroyWebcFailSafe(IDfWebCacheConfig config) {
        try {
            if (simpleDestroyWebc(config)) return;
            if (destroyWebcWithJobCreation(config)) return;
            // TODO: not covered all cases: target
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, Level.WARN, "destroyWebcFailSafe(). Unknown error", e);
        }
    }

    private static boolean simpleDestroyWebc(IDfWebCacheConfig config) {
        try {
            if (config.isCheckedOut()) {
                config.cancelCheckout();
            }
            config.destroy();
            return true;
        } catch (Exception e) {
            LOG.debug("simple config.destroy() failed", e);
        }
        return false;
    }

    private static boolean destroyWebcWithJobCreation(IDfWebCacheConfig config) {
        try {
            IDfSession session = SessionManagerHandler.getInstance().getMainSession();
            String configId = config.getObjectId().getId();
            IDfJob job = (IDfJob) session.getObjectByQualification("dm_job where any method_arguments = '-config_object_id " + configId + "'");
            if (job == null) {
                createPublishJob(config);
            } else {
                if (job.isCheckedOut()) {
                    job.cancelCheckout();
                    // cancelCheckout is designed to refersh objects after unlocking, but
                    // for some reason this doesn't work here.
                    // Check if the following help.
                    job.save();
                    Util.sleepSilently(DEFAULT_INTERVAL);
                }
            }
            config.destroy();
            return true;
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, Level.WARN, MSG_ERROR, e);
        }
        return false;
    }


    public static boolean isWebcSlow() {
        String isSlow = PropertiesHolder.getInstance().getProperty(PROP_PROCESS_SLOW_CONFIGS);
        return BooleanUtils.toBoolean(isSlow);
    }


    public static String getParentConfigName(String configName) {
        if (configName.startsWith(TEMP_PREFIX)) {
            configName = StringUtils.replace(configName, TEMP_PREFIX, EMPTY_STRING);
            return StringUtils.substringBeforeLast(configName, DASH) + DASH + APPROVED_LABEL.toUpperCase();
        }
        return configName;
    }

    private static String getJurisdiction(String configName) {
        return getParentConfigName(configName).substring(0, 2);
    }

}
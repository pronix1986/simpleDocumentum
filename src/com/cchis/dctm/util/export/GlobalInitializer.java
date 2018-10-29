package com.cchis.dctm.util.export;

import com.cchis.dctm.util.export.exception.ExportException;
import com.cchis.dctm.util.export.report.*;
import com.cchis.dctm.util.export.util.Util;
import com.documentum.admin.object.IDfJob;
import com.documentum.admin.object.IDfWebCacheConfig;
import com.documentum.admin.object.IDfWebCacheTarget;
import com.documentum.fc.client.*;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.IDfId;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.Statement;
import java.util.*;

import static com.cchis.dctm.util.export.util.ExportConstants.*;


public final class GlobalInitializer {
    private static final Logger LOG = Logger.getLogger(GlobalInitializer.class);
    private static final GlobalInitializer INSTANCE = new GlobalInitializer();

/*    private static final int PUBLISH_TIMEOUT_MIN_DEFAULT = 60;
    private static final int PUBLISH_TIMEOUT_DEFAULT = 120;
    private static final int PUBLISH_TIMEOUT_MAX_DEFAULT = 180;*/
    private static final int PUBLISH_TIMEOUT_MIN_DEFAULT = 120;
    private static final int PUBLISH_TIMEOUT_DEFAULT = 180;
    private static final int PUBLISH_TIMEOUT_MAX_DEFAULT = 210;

    private final IDfSession giSession;
    private final List<IndexId> indexes = new ArrayList<>();


    private GlobalInitializer() {
        giSession = SessionManagerHandler.getInstance().getMainSession();


        indexes.add(new IndexId("dmr_content_s", "i_parked_state")); // absolutely necessary. Used in publishing !
        indexes.add(new IndexId("dm_sysobject_r", Collections.singletonList("r_version_label"), Collections.singletonList("r_object_id"))); // used in finding versions
        // other indexes may or may not improve performance or even may worse it for some reason.
//        indexes.add(new IndexId("dm_sysobject_s", "i_contents_id"));
//        indexes.add(new IndexId("dm_sysobject_s", Collections.singletonList("i_is_deleted"), Collections.singletonList("r_object_id"))); // this index relates to dctmcount query and I left out direct call to db
//        indexes.add(new IndexId("dm_registered_s", "table_name"));

        LOG.trace("indexes: " + indexes);
    }

    public static GlobalInitializer getInstance() {
        return INSTANCE;
    }

    private void setSCSExtraArgs() {
        try {
            IDfSysObject scsConfig = (IDfSysObject) giSession.getObjectByQualification(DQL_QUAL_SCS_CONFIG);
            if(SET_SCS_EXTRA_ARGS) {
                scsConfig.setRepeatingString(ATTR_EXTRA_ARGS, 0, "sync_on_zero_updates TRUE");
                scsConfig.setRepeatingString(ATTR_EXTRA_ARGS, 1, "source_attrs_only TRUE");
                //scsConfig.setRepeatingString(ATTR_EXTRA_ARGS, 2, "publish_source_version_labels TRUE"); // Do not work for some reason
                LOG.info(MSG_SCS_EXTRA_ARGS_SET);
            }
            if (PublishType.shouldGetExportSetOnly()) {
                scsConfig.setString(ATTR_JDBC_DRIVER, INVALID_JDBC_DRIVER);
                LOG.info(MSG_SCS_JDBC_DRIVER_SET);
            }
            scsConfig.save();

        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
            throw new ExportException(e);
        }
    }

    private void unsetSCSExtraArgs() {
        try {
            IDfSysObject scsConfig = (IDfSysObject) giSession.getObjectByQualification(DQL_QUAL_SCS_CONFIG);
            if(SET_SCS_EXTRA_ARGS) {
                scsConfig.removeAll(ATTR_EXTRA_ARGS);
                LOG.info(MSG_SCS_EXTRA_ARGS_UNSET);
            }
            if (PublishType.shouldGetExportSetOnly()) {
                scsConfig.setString(ATTR_JDBC_DRIVER, SQL_JDBC_DRIVER);
                LOG.info(MSG_SCS_JDBC_DRIVER_UNSET);
            }
            scsConfig.save();

        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
        }
    }

    private void setPublishTimeout() {
        try {
            IDfMethodObject methodObject = (IDfMethodObject) giSession.getObjectByQualification(DQL_QUAL_DM_WEBPUBLISH_METHOD);
            int timeout = Integer.parseInt(PUBLISH_TIMEOUT);
            methodObject.setTimeoutMin(timeout - 1);
            methodObject.setTimeoutDefault(timeout);
            methodObject.setTimeoutMax(timeout + 1);
            methodObject.save();

            LOG.info(MSG_DM_WEBCACHE_PUBLISH_SET);
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
            throw new ExportException(e);
        }
    }

    private void unsetPublishTimeout() {
        try {
            IDfMethodObject methodObject = (IDfMethodObject) giSession.getObjectByQualification(DQL_QUAL_DM_WEBPUBLISH_METHOD);

            // default values 60/120/180
            // due to problems in prod I changed default values to 120/180/210
            methodObject.setTimeoutMin(PUBLISH_TIMEOUT_MIN_DEFAULT);
            methodObject.setTimeoutDefault(PUBLISH_TIMEOUT_DEFAULT);
            methodObject.setTimeoutMax(PUBLISH_TIMEOUT_MAX_DEFAULT);
            methodObject.save();

            LOG.info(MSG_DM_WEBCACHE_PUBLISH_UNSET);
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
        }
    }

    private void createMissedImageConfig(String configName, String folderPath) { // Main session is used. It seems to be ok here
        try {
            IDfWebCacheConfig templateConfig = (IDfWebCacheConfig) giSession.getObjectByQualification(DQL_QUAL_GET_TEMPLATE_IMAGE_CONFIG);
            templateConfig.initialize(giSession, templateConfig.getObjectId());

            IDfId folderId = giSession.getIdByQualification(String.format(DQL_QUAL_GET_FOLDER_ID, folderPath));

            IDfWebCacheTarget target = templateConfig.getWebCacheTarget(0);
            String parentRootDir = target.getTargetRootDir();
            String parentVirtualDir = target.getTargetVirtualDir();
            String parentDBTablename = target.getPropDBTablename();

            IDfId newConfigId = templateConfig.saveAsNew();
            IDfWebCacheConfig newConfig = (IDfWebCacheConfig) giSession.getObject(newConfigId);
            newConfig.setObjectName(configName);
            newConfig.setTitle(MISSED_CONFIG_TITLE);
            newConfig.setSourceFolderID(folderId);
            newConfig.setLogEntry(folderPath);

            IDfWebCacheTarget newTarget = newConfig.getWebCacheTarget(0);
            newTarget.setObjectName(configName);
            newTarget.setTargetRootDir(parentRootDir);
            newTarget.setTargetVirtualDir(parentVirtualDir);
            newTarget.setPropDBTablename(parentDBTablename);

            newConfig.save();

            LOG.info(String.format("Missed config created: %s", ConfigId.getConfigId(newConfig)));
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
            throw new ExportException(e);
        }
    }

    private void createMissedImagesConfigs() {
        destroyStaleConfigs();
        createMissedImageConfig(ROOT_IMAGE_CONFIG_NAME, ROOT_IMAGE_PATH);
        createMissedImageConfig("IC-INSOURCE-IMAGES-APPROVED", "/icspipeline.com/IC/INSource/Images");
    }

    private void checkWebcsVersions() {
        try {
            IDfEnumeration enumeration = giSession.getObjectsByQuery(DQL_GET_ALL_SCS_CONFIGS_WRONG_VERSION_LABEL, DCTM_WEBC_CONFIG);
            while (enumeration.hasMoreElements()) {
                IDfWebCacheConfig config = (IDfWebCacheConfig) enumeration.nextElement();
                ConfigId cId = ConfigId.getConfigId(config);
                String version = cId.getVersion();
                if (APPROVED_LABEL.equals(version)) {
                    continue;
                }
                WebcHandler.initConfig(config);
                WebcHandler.resetWebc(config);
                LOG.debug("Set Approved version for " + cId);
            }
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
        }
    }

    private void destroyStaleConfigs() {
        LOG.debug("Destroying stale configs");
        try {
            IDfEnumeration configs = giSession.getObjectsByQuery(DQL_STALE_CONFIGS, DCTM_WEBC_CONFIG);
            while (configs.hasMoreElements()) {
                IDfWebCacheConfig config = (IDfWebCacheConfig) configs.nextElement();
                WebcHandler.destroyWebcFailSafe(config);
            }
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
        }
        destroyStaleTargets();
        destroyStaleJobs();
    }

    private void destroyStaleTargets() {
        LOG.debug("Destroying stale targets");
        IDfCollection collection = null;
        try {
            collection = Util.runQuery(giSession, DQL_STALE_TARGETS); // session.getObjectsByQuery doesn't work for some reason
            while (collection.next()) {
                try {
                    IDfWebCacheTarget target = (IDfWebCacheTarget) giSession.getObject(collection.getId(DCTM_R_OBJECT_ID));
                    target.destroy();
                }  catch (Exception ex) {
                    LogHandler.logWithDetails(LOG, MSG_ERROR, ex);
                }
            }
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
        } finally {
            Util.closeCollection(collection);
        }
    }

    private void destroyStaleJobs() {
        LOG.debug("Destroying stale jobs");
        try {
            IDfEnumeration jobs = giSession.getObjectsByQuery(DQL_STALE_JOBS, DCTM_DM_JOB);
            while (jobs.hasMoreElements()) {
                IDfJob job = (IDfJob) jobs.nextElement();
                try {
                    job.destroy();
                } catch (DfException e) {
                    LogHandler.logWithDetails(LOG, MSG_ERROR, e);
                }
            }
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
        }
    }

    private void createIndexes() {
        execIndexes(SQL_CREATE_INDEXES, MSG_CREATE_INDEXES);
        LOG.info("Index(es) created.");
    }

    private void dropIndexes() {
        execIndexes(SQL_DROP_INDEXES, MSG_DROP_INDEXES);
        LOG.info("Index(es) dropped.");
    }

    private void execIndexes(String sql, String logMessage) {
        Connection conn = SqlConnector.getInstance().getSqlConnection();
        Statement statement = null;
        try {
            statement = conn.createStatement();
            for (IndexId index: indexes) {
                String tableName = index.getTableName();
                List<String> columns = index.getColumns();
                List<String> include = index.getInclude();
                String sqlStat = String.format(sql, index.toString(), tableName,
                        Util.simpleArrayToStringAsCSV(columns.toString()),
                        Util.isNotEmptyCollection(include)
                                ? String.format(SQL_CREATE_INDEXES_INCLUDE, Util.simpleArrayToStringAsCSV(include.toString()))
                                : EMPTY_STRING);
                statement.addBatch(sqlStat);
                LOG.trace(sqlStat);
            }
            SqlConnector.executeBatch(statement, logMessage);
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
        } finally {
            SqlConnector.closeSilently(statement);
        }
    }

    void init() {
        if (shouldInit()) {
            checkSingletones();
            WebcHandler.destroyStaleRegAndUnregTables();
            setSCSExtraArgs();
            setPublishTimeout();
            createMissedImagesConfigs();
            if (CREATE_INDEXES) createIndexes();
        }
    }

    void toDefault() {
        if (shouldInit()) {
            unsetSCSExtraArgs();
            unsetPublishTimeout();
            destroyStaleConfigs();
            if (!PublishType.shouldCreateNewConfig()) {
                checkWebcsVersions();
            }
            if (CREATE_INDEXES) dropIndexes();
        }
        SqlConnector.getInstance().closeSqlConnection();
    }

    private void checkSingletones() {
        SqlConnector.getInstance();
        CountTimeReport.getInstance();
        ReportHandler.getInstance();
        MaybeErrorReport.getInstance();
        DuplicateNamesReport.getInstance();
        CountTimeReport.getInstance();
        ConfigReport.getTotalReportInstance();
        BookStartDateReport.getInstance();
    }

    private boolean shouldInit() {
        return GLOBAL_INIT && USE_PUBLISHING;
    }

}

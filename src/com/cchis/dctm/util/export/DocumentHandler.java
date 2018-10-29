package com.cchis.dctm.util.export;

import com.cchis.dctm.util.export.exception.ExportException;
import com.cchis.dctm.util.export.util.StopWatchEx;
import com.cchis.dctm.util.export.util.Util;
import com.documentum.admin.object.IDfWebCacheConfig;
import com.documentum.fc.client.*;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfId;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.*;

import static com.cchis.dctm.util.export.util.ExportConstants.*;

public class DocumentHandler {

    private static final Logger LOG = Logger.getLogger(DocumentHandler.class);

    private DocumentHandler() { }

    static void destroySysObjects(IDfSession session, String dql, String logMessage) {
        IDfCollection collection = null;
        Counter counter = new Counter(false);
        try {
            collection = Util.runQuery(session, dql);
            while (collection.next()) {
                destroySysObjectById(session, collection.getString(DCTM_R_OBJECT_ID), counter);
            }
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
        } finally {
            LOG.trace(String.format(logMessage, counter.getSuccess(), counter.getFailed()));
            Util.closeCollection(collection);
        }
    }

    public static void destroySysObjects(IDfSession session, String dql) {
        destroySysObjects(session, dql, MSG_DESTROY_DOCUMENT);
    }

    private static void destroySysObjectById(IDfSession session, String id, Counter counter) {
        try {
            destroySysObjectById(session, id);
            counter.incrementSuccess();
        } catch (Exception e) {
            counter.incrementFailed();
            LogHandler.logWithDetails(LOG, Level.WARN, MSG_ERROR, e);
        }
    }

    private static void destroySysObjectById(IDfSession session, String id) throws DfException {
        IDfSysObject document = (IDfSysObject) session.getObject(new DfId(id));
        //LOG.trace(String.format(MSG_DESTROY_DOCUMENT, document.getObjectId().getId(), document.getObjectName()));
        destroySysObject(document);
    }

    private static void destroySysObject(IDfSysObject document) throws DfException {
        if (document.isCheckedOut()) {
            LOG.debug(String.format(MSG_LOCK_OWNER, document.getLockOwner()));
            document.cancelCheckout();
        }
        document.destroy();
    }

    public static String getLastApprovedVersion() {
        IDfSession session = SessionManagerHandler.getInstance().getMainSession();
        try {
            IDfSysObject doc = (IDfSysObject) session.getObjectByQualification(DQL_QUAL_GET_LAST_APPROVED);
            return getNumericVersionLabel(doc);
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
            throw new ExportException(e.getMessage());
        }
    }

    public static boolean isBranchVersion(String version) {
        return version.matches(R_BRANCH_VERSION_LABEL_PATTERN);
    }


    public static Set<String> getAllNumericVersionsUnderBook(IDfWebCacheConfig config) {
        IDfCollection collection = null;
        Set<String> versions = new TreeSet<>();
        try {
            String folderPath = WebcHandler.getPublishFolder(config);
            String dql = String.format(DQL_GET_ALL_VERSIONS, WebcHandler.getPublishType(config), folderPath);
            dql = PublishType.adjustDql(dql, config);
            LOG.trace(dql);
            collection = Util.runQuery(config.getObjectSession(), dql);
            while (collection.next()) {
                try {
                    String versionLabel = collection.getString(DCTM_R_VERSION_LABEL);
                    if (StringUtils.isNotEmpty(versionLabel)
                            && versionLabel.matches(R_VERSION_LABEL_PATTERN)) {
                        versions.add(versionLabel);
                    }
                } catch (Exception e) {
                    LogHandler.logWithDetails(LOG, MSG_ERROR, e);
                }
            }
            return versions;
        } catch(Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
            throw new ExportException(e.getMessage());
        } finally {
            Util.closeCollection(collection);
        }
    }

    private static String getNumericVersionLabel(IDfSysObject doc) throws DfException {
        return doc.getVersionLabel(0);
    }

    public static List<String> getVersionLabelsOfDocument(IDfSysObject doc) throws DfException {
        List<String> versions = new ArrayList<>();
        IDfVersionLabels vl = doc.getVersionLabels();
        for (int i = 0; i < vl.getVersionLabelCount(); i++) {
            versions.add(vl.getVersionLabel(i));
        }
        return versions;
    }


    public static String getVersionLabelsString(IDfSysObject doc) throws DfException {
        return doc.getAllRepeatingStrings("r_version_label", SEMICOLON);
    }

    public static String getVersionLabelsString(IDfSession session, String docId) {
        try {
            IDfSysObject doc = (IDfSysObject) session.getObject(new DfId(docId));
            return getVersionLabelsString(doc);
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
            throw new ExportException(e.getMessage());
        }
    }

    public static boolean isReadyToImport(IDfSysObject doc) throws DfException {
        String versionsStr = getVersionLabelsString(doc);
        return versionsStr.contains(CURRENT_LABEL)
                || versionsStr.contains(APPROVED_LABEL)
                || doc.getCurrentState() == 3;
    }

    public static boolean isReadyToImport(IDfSession session, String docId) {
        try {
            IDfSysObject doc = (IDfSysObject) session.getObject(new DfId(docId));
            return isReadyToImport(doc);
        } catch (DfException e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
            throw new ExportException(e.getMessage());
        }
    }

    public static int countDocumentsUnderBook(IDfWebCacheConfig config, boolean useCache) {
        return countDocumentsUnderBookWithVersion(config.getSession(), config, null, useCache);
    }

    public static int countDocumentsUnderBook(IDfWebCacheConfig config) {
        return countDocumentsUnderBook(config, USE_BOOK_CACHE);
    }

    public static int countDocumentsUnderBookWithVersion(IDfSession session, IDfWebCacheConfig config, String version, boolean useCache) {
        try {
            String folderPath = WebcHandler.getPublishFolder(config, useCache);
            String dql = String.format(DQL_COUNT_DOCUMENTS, WebcHandler.getPublishType(config), folderPath);
            if (StringUtils.isNotEmpty(version)) {
                dql += String.format(DQL_WITH_VERSION_POSTFIX, version);
            }
            dql = PublishType.adjustDql(dql, config);
            dql = Util.appendDqlHints(dql, DQL_HINT_UNCOMMITTED_READ, DQL_HINT_SQL_DEF_RESULT_SETS);
            return Util.getCount(session, dql);
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
            throw new ExportException(e.getMessage());
        }
    }

    public static int countDocumentsUnderBookWithVersion(IDfSession session, IDfWebCacheConfig config, String version) {
        return countDocumentsUnderBookWithVersion(session, config, version, true);
    }

    public static int countDocumentsUnderBooks(List<IDfWebCacheConfig> configs) {
        StopWatchEx stopWatch = StopWatchEx.createStarted();
        int count = 0;
        for (IDfWebCacheConfig config: configs) {
            count += countDocumentsUnderBook(config, false);
        }
        LOG.debug("countDocumentsUnderBooks(). Time taken: " + stopWatch.stopAndGetTime());
        return count;
    }

    private static boolean isFolderNameValid(String folderName) {
        String[] parts = folderName.split(SLASH);
        return folderName.toLowerCase().startsWith(PIPELINE)
                && parts.length >= 5;
    }

    public static List<String> getModifiedRecordsIdsUnderBookWithVersion(IDfWebCacheConfig config, String version) {
        List<String> ids = new ArrayList<>();
        IDfSession session = SessionManagerHandler.getInstance().getMainSession();
        IDfCollection collection = null;
        try {
            String folderName = WebcHandler.getPublishFolder(config);
            String publishType = WebcHandler.getPublishType(config);
            String dql = String.format(DQL_GET_DOCUMENTS, publishType, folderName);
            if (StringUtils.isNotEmpty(version)) {
                dql += String.format(DQL_WITH_VERSION_POSTFIX, version);
            }
            dql = PublishType.adjustDql(dql, config);
            collection = Util.runQuery(session, dql);
            while (collection.next()) {
                ids.add(collection.getString(DCTM_R_OBJECT_ID));
            }
            return ids;
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
            throw new ExportException(e.getMessage());
        } finally {
            Util.closeCollection(collection);
        }
    }

    /**
     * Trying to avoid Stack Overflow Error for large list of ids.
     * Should return at least one batch (probably empty one)
     * @param config
     * @param version
     * @param batchCapacity
     * @return
     */
    public static List<List<String>> getBatchedModifiedRecordsIdsUnderBookWithVersion(IDfWebCacheConfig config, String version, int batchCapacity) throws DfException {
        List<List<String>> partitions = new ArrayList<>();
        if (!USE_PUBLISHING || StringUtils.isNotEmpty(PublishType.getCombDate(config.getObjectName()))) {
            partitions = Util.listPartition(getModifiedRecordsIdsUnderBookWithVersion(config, version), batchCapacity);
        } else {
            partitions.add(new ArrayList<>());
        }
        return partitions;
    }

    public static String getFolderName(IDfSysObject obj) throws DfException {
        return getFolderName(obj, null);
    }

    public static String getFolderName(IDfSysObject obj, IDfWebCacheConfig config) throws DfException {

        // com.documentum.web.formext.docbase.FolderUtil.getPrimaryFolderPath(id, true)
        String startsWith = config != null ? WebcHandler.getPublishFolder(config) : PIPELINE;

        IDfSession session = obj.getSession();
        IDfFolder folder = (IDfFolder) session.getObject(obj.getFolderId(0));
        String folderPath = folder.getFolderPath(0);
        if (folderPath.startsWith(startsWith)) {
            return folderPath;
        }
        for (int i = 0; i < obj.getFolderIdCount(); i++) {
            folder = (IDfFolder) session.getObject(obj.getFolderId(i));
            for (int j = 0; j < folder.getFolderPathCount(); j++) {
                String folderPathi = folder.getFolderPath(j);
                if (folderPathi.startsWith(startsWith)) {
                    return folderPathi;
                }
            }
        }
        LOG.warn("Cannot find folder path under root web cabinet: " + folderPath);
        return folderPath;

    }

    public static String getRelativePath(IDfSysObject obj, IDfWebCacheConfig config) throws DfException {
        String sourceFolderPath = WebcHandler.getPublishFolder(config);
        return DocumentHandler.getFolderName(obj).substring(sourceFolderPath.length()) + obj.getObjectName();
    }

}

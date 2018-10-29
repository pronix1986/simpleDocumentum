package com.cchis.dctm.util.export.report;

import com.cchis.dctm.util.export.DocumentHandler;
import com.cchis.dctm.util.export.LogHandler;
import com.cchis.dctm.util.export.SessionManagerHandler;
import com.cchis.dctm.util.export.WebcHandler;
import com.cchis.dctm.util.export.util.Util;
import com.documentum.admin.object.IDfWebCacheConfig;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.client.content.IDfContent;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.IDfId;
import com.documentum.wcm.IWcmAppContext;
import com.documentum.wcm.type.IWcmContent;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

import static com.cchis.dctm.util.export.util.ExportConstants.*;

public class ReportRecord {

    private static final Logger LOG = Logger.getLogger(ReportRecord.class);
    protected static final ReportRecord DUMMY_RECORD =
            new ReportRecord(DCTM_I_CHRONICLE_ID, DCTM_R_OBJECT_ID, DCTM_OBJECT_NAME, DCTM_R_FOLDER_PATH, VERSION_LABELS, DCTM_R_CURRENT_STATE, FORMAT);
    private static final String MSG_ERROR_CANNOT_INSTANTIATE_RECORD = "Cannot instantiate record: %s";

    private String chronicleId;
    private String id;
    private List<String> duplicateNameIds;
    private String objectName;
    private String folderPath;
    private String filteredObjectName;
    private String filteredFolderPath;
    private IDfWebCacheConfig config;

    private String version;
    private IDfSysObject object;
    private String currentState; // I don't need for r_current_state as integer. String is more convenient.
    private String fullFormat;

    protected ReportRecord () { }

    /**
     * Only for 'dummy' records.
     */
    public ReportRecord (String chronicleId, String id, String objectName, String folderPath, String version, String currentState, String fullFormat) {
        this.chronicleId = chronicleId;
        this.id = id;
        this.objectName = objectName;
        this.folderPath = folderPath;
        this.version = version;
        this.currentState = currentState;
        this.fullFormat = fullFormat;
    }

    /**
     * Not in use now
     */
    public ReportRecord(IDfSysObject object) throws DfException {
        try {
            this.object = object;
            this.id = object.getObjectId().getId();
            chronicleId = object.getChronicleId().getId();
            objectName = object.getObjectName();
            folderPath = DocumentHandler.getFolderName(object);
            version = DocumentHandler.getVersionLabelsString(object);
            IDfContent content = (IDfContent) object.getSession().getObject(object.getContentsId());
            fullFormat = content.getFullFormat();
            currentState = String.valueOf(object.getCurrentState());
        } catch (DfException e) {
            LOG.debug(String.format(MSG_ERROR_CANNOT_INSTANTIATE_RECORD, id));
            throw e;
        }
    }


    public ReportRecord (IDfSession session, IDfId id, IDfWebCacheConfig config) throws DfException {
        try {
            this.id = id.getId();
            object = (IDfSysObject) session.getObject(id);
            chronicleId = object.getChronicleId().getId();
            objectName = object.getObjectName();
            this.config = config;
            folderPath = DocumentHandler.getFolderName(object, config);
            version = DocumentHandler.getVersionLabelsString(object);
            IDfContent content = (IDfContent) session.getObject(object.getContentsId());
            fullFormat = content.getFullFormat();
            currentState = String.valueOf(object.getCurrentState());
        } catch (DfException e) {
            LOG.debug(String.format(MSG_ERROR_CANNOT_INSTANTIATE_RECORD, id));
            throw e;
        }
    }

    public ReportRecord (IDfSession session, IDfId id) throws DfException {
        this(session, id, null);
    }

    public ReportRecord (ReportRecord copy) {
        this.chronicleId = copy.chronicleId;
        this.id = copy.id;
        this.objectName = copy.objectName;
        this.filteredObjectName = copy.filteredObjectName;
        this.filteredFolderPath = copy.filteredFolderPath;
        this.folderPath = copy.folderPath;
        this.version = copy.version;
        this.fullFormat = copy.fullFormat;
        this.currentState = copy.currentState;
        if (copy.duplicateNameIds != null) {
            this.duplicateNameIds = new ArrayList<>(copy.duplicateNameIds.size());
            duplicateNameIds.addAll(copy.duplicateNameIds);
        }
    }

    public IDfSysObject getObject() {
        return object;
    }

    public String getObjectName() {
        return objectName;
    }

    public String getFilteredObjectName() {
        return filteredObjectName;
    }

    public String getPublishedName() {
        try {
            IWcmAppContext context = SessionManagerHandler.getInstance().getWcmAppContext();
            IWcmContent content = (IWcmContent) context.getObject(object.getObjectId(), object.getSession());
            return content.getPublishedName(fullFormat, null, config.getObjectId().getId());
        } catch (DfException e) {
            LogHandler.logWithDetails(LOG, "getPublishedName()", e);
        }
        return filteredObjectName;
    }


    public String getFullFormat() {
        return fullFormat;
    }

    public void setFolderPath(String folderPath) {
        this.folderPath = folderPath;
    }

    public String getFilteredObjectNameNullSafe() {
        return StringUtils.isEmpty(filteredObjectName) ? objectName : filteredObjectName;
    }

    public List<String> getDuplicateNameIds() {
        return duplicateNameIds;
    }

    public void setObjectName(String objectName) {
        this.objectName = objectName;
    }

    public String getFolderPath() {
        return folderPath;
    }

    public String getChronicleId() {
        return chronicleId;
    }

    public boolean setFilteredPath(String name, String folderPath) {
        if (!this.objectName.equals(name) || !this.folderPath.equals(folderPath)) {
            LOG.trace(String.format("Setting new values: %s, %s", folderPath, name));
            filteredObjectName = name;
            filteredFolderPath = folderPath;
            return true;
        }
        return false;
    }

    String getPath() {
        return folderPath + SLASH + objectName;
    }

    public String getFilteredPath() {
        return (filteredFolderPath == null || filteredObjectName == null)
                ? null : filteredFolderPath + SLASH + filteredObjectName;
    }

    public String getFilteredPathNullSafe() {
        return StringUtils.isEmpty(getFilteredPath()) ? getPath() : getFilteredPath();
    }

    public String getRelativePath(IDfWebCacheConfig config) throws DfException {
        String sourceFolderPath = WebcHandler.getPublishFolder(config);
        return getFilteredPathNullSafe().substring(sourceFolderPath.length() + 1);
    }


    public String getId() {
        return id;
    }

    public String getVersion() {
        return version;
    }

    public String getCurrentState() {
        return currentState;
    }

    void setDuplicateNameIds(List<String> duplicateNameIds) {
        this.duplicateNameIds = duplicateNameIds;
    }

    public boolean isReadyToImport() throws DfException {
        if (object != null) {
            return DocumentHandler.isReadyToImport(object);
        } else {
            IDfSession session = SessionManagerHandler.getInstance().getMainSession();
            return DocumentHandler.isReadyToImport(session, id);
        }
    }

    public boolean isReadyToImportFailSafe() {
        try {
            return isReadyToImport();
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, "isReadyToImportFailSafe()", e);
        }
        return false;
    }

    public boolean isApproved() {
        return version.contains(APPROVED_LABEL);
    }

    public boolean isCurrent() {
        return version.contains(CURRENT_LABEL);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReportRecord record = (ReportRecord) o;
        return id.equals(record.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(String.format("%s,%s,", chronicleId, id));
        if(Util.isNotEmptyCollection(duplicateNameIds)) {
            builder.append(duplicateNameIds);
            builder.append(COMMA);
        }
        if(StringUtils.isEmpty(getFilteredPath())) {
            builder.append(Util.escapeCommaCSV(getPath()));
        } else {
            builder.append(Util.escapeCommaCSV(getPath() + ARROW + getFilteredPath()));
        }

        builder.append(COMMA);
        builder.append(version);
        if(StringUtils.isNotEmpty(currentState)) {
            builder.append(COMMA);
            builder.append(currentState);
        }
        if(StringUtils.isNotEmpty(fullFormat)) {
            builder.append(COMMA);
            builder.append(fullFormat);
        }
        return builder.toString();
    }

    static List<String> getPaths(List<ReportRecord> records) {
        return records.stream().map(ReportRecord::getPath).collect(Collectors.toList());
    }

    public static Map<String, List<ReportRecord>> getFilteredPathsMap(List<ReportRecord> records) {
        return records.stream().collect(Collectors.groupingBy(ReportRecord::getFilteredPath));
    }

    public static List<ReportRecord> deepCopy(List<ReportRecord> records) {
        return records.stream().map(ReportRecord::new).collect(Collectors.toList());
    }

    public static List<ReportRecord> getReadyToImport(List<ReportRecord> records) {
        return records.stream().filter(ReportRecord::isReadyToImportFailSafe).collect(Collectors.toList());
    }

    public static void addAllCopies (List<ReportRecord> records, List<ReportRecord> toAdd) {
        records.addAll(toAdd.stream().map(ReportRecord::new).collect(Collectors.toList()));
    }

    public static List<String> getHeaders() {
        List<String> headers = new ArrayList<>();
        headers.add(DCTM_I_CHRONICLE_ID);
        headers.add(DCTM_R_OBJECT_ID);
        headers.add("Absolute Path");
        headers.add(VERSION_LABELS);
        headers.add(DCTM_R_CURRENT_STATE);
        headers.add(FORMAT);
        return headers;
    }

}

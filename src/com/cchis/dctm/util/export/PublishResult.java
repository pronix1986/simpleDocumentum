package com.cchis.dctm.util.export;

import com.cchis.dctm.util.export.report.AbstractReport;
import com.cchis.dctm.util.export.util.FileUtil;
import com.cchis.dctm.util.export.util.Util;
import com.documentum.admin.object.IDfWebCacheConfig;
import com.documentum.fc.client.IDfDocument;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfId;
import com.documentum.fc.common.IDfId;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

import static com.cchis.dctm.util.export.util.ExportConstants.*;

/**
 * May have at least 3 states:
 * <ul>
 *     <li>OK: completed without errors in ExportUtil, Documentum state is 'Completed successfully'</li>
 *     <li>Maybe error: completed without errors in ExportUtil, but Documentum state is not 'Completed successfully' or 'Unknown Problem' records were found</li>
 *     <li>Error: got exception in ExportUtil</li>
 * </ul>
 */
public class PublishResult {
    private static final Logger LOG = Logger.getLogger(PublishResult.class);
    private static final String MSG_ERROR_CANNOT_CREATE = "Cannot create PublishResult";

    private ConfigId cId;

    private IDfWebCacheConfig config;

    private boolean isOk = false;
    private boolean isError = false;
    private Throwable throwable;

    private int batch;
    private int batchesCount;
    private boolean singleBatch;
    private static final ThreadLocal<List<BatchId>> batches = ThreadLocal.withInitial(() -> new ArrayList<>(0));

    private String expectedWebcCompletedStatus;

    public PublishResult(IDfWebCacheConfig config) {
        this(config, null);
    }

    public PublishResult(IDfWebCacheConfig config, List<PublishResult> results) {
        try {
            initPublishResult(config, results);
            this.batch = 0;
            this.singleBatch = true;
        } catch (Exception e) {
            LOG.error(MSG_ERROR_CANNOT_CREATE, e);
        }
    }

    public PublishResult(IDfWebCacheConfig config, List<PublishResult> results, int batch, int batchesCount) {
        try {
            initPublishResult(config, results);
            this.batchesCount = batchesCount;
            this.batch = batch;
            this.singleBatch = batchesCount == 1;
        } catch (Exception e) {
            LOG.error(MSG_ERROR_CANNOT_CREATE, e);
        }
    }

    private void initPublishResult(IDfWebCacheConfig config, List<PublishResult> results) throws DfException {
        this.config = config;
        this.cId = ConfigId.getConfigId(config);
        if (results != null) {
            results.add(this);
        }
        this.expectedWebcCompletedStatus = PublishType.getWebcCompletedStatus();
    }


    public boolean isOk() {
        return isOk;
    }

    public boolean isNotOk () {
        return !isOk;
    }

    public void setOk(boolean ok) {
        isOk = ok;
        isError = false;
        throwable = null;
    }

    public void setOk() {
        setOk(true);
    }

    public void setNotOk() {
        setOk(false);
    }

    public boolean isError() {
        return isError;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public void setError(boolean error, Throwable t) {
        isOk = false;
        isError = error;
        throwable = t;
    }

    public void setError(Throwable throwable) {
        setError(true, throwable);
    }

    public boolean isMaybeError() {
        return !isOk && !isError;
    }

    public String getConfigId() {
        return cId.getConfigId();
    }

    public String getConfigName() {
        return cId.getConfigName();
    }

    public ConfigId getcId() {
        return cId;
    }

    public IDfWebCacheConfig getConfig() {
        return config;
    }

    public int getBatch() {
        return batch;
    }

    public boolean isSingleBatch() {
        return singleBatch;
    }

    /**
     * should be true for singleBatch
     * @return
     */
    public boolean isFirstBatch() {
        return singleBatch || (batch == 0);
    }

    public boolean isLastBatch() {
        return singleBatch || (batch == (batchesCount - 1));
    }

    public int getBatchesCount() {
        return batchesCount;
    }

    public static void clearBatches() {
        getBatchDirs().forEach(FileUtil::deleteQuietly);
        batches.get().clear();
    }

    public static void addBatch(BatchId id) {
        batches.get().add(id);
    }


    public void preserveResponce() {
        IDfSession session = config.getSession();
        IDfId responseDocId = new DfId(null);
        try {
            responseDocId = session.getIdByQualification(String.format(DQL_QUAL_TOP_SCS_LOG, cId.getConfigName()));
        } catch (DfException ignore) { }

        if (responseDocId != null && !responseDocId.isNull()) {
            ByteArrayInputStream bais = null;
            try {
                // initially responseDocId came from external resource. Haven't done merge yet.
                IDfDocument document = (IDfDocument) session.getObject(responseDocId);
                String objectName = document.getObjectName();
                long prefix = Calendar.getInstance().getTimeInMillis();
                String logName = prefix + UNDERSCORE + objectName;
                File destination = new File(AbstractReport.getReportPath(ERROR_RESPONSES, logName, ".txt", false));
                FileUtil.mkdirs(destination.getParentFile());
                bais = document.getContent();
                FileUtil.copyInputStreamToFile(bais, destination);
            } catch (Exception e) {
                LogHandler.logWithDetails(LOG, String.format("Cannot preserve log file for %s. Doc Id: %s", cId, responseDocId), e);
            } finally {
                IOUtils.closeQuietly(bais);
            }
        }
    }

    public static List<File> getBatchDirs() {
        return batches.get().stream().map(BatchId::getExportSet).collect(Collectors.toList());
    }

    public static int getTotalCount() {
        return batches.get().stream().mapToInt(BatchId::getCount).sum();
    }

    public String getWebcCompletedStatus() {
        return expectedWebcCompletedStatus;
    }

    public void setWebcCompletedStatus(String expectedWebcCompletedStatus) {
        this.expectedWebcCompletedStatus = expectedWebcCompletedStatus;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(getConfigId());
        builder.append(Util.wrap(getConfigName(), COMMA));
        if(!isOk) {
            builder.append("Not ");
        }
        builder.append("OK");
        builder.append(COMMA);
        if (!isError) {
            builder.append("No ");
        }
        builder.append("Error");
        if(!singleBatch) {
            builder.append(COMMA);
            builder.append("Batch: " + batch);
        }
        return builder.toString();
    }

    public static List<String> getConfigIds(List<PublishResult> results) {
        return results.stream().map(PublishResult::getConfigId).collect(Collectors.toList());
    }

    public static Map<String, String> getConfigIdStatusMap(List<PublishResult> results) {
        return results.stream().filter(PublishResult::isLastBatch)
                .collect(Collectors.toMap(PublishResult::getConfigId, PublishResult::getWebcCompletedStatus));
    }

    public static List<ConfigId> getcIds(List<PublishResult> results) {
        return results.stream().map(PublishResult::getcId).collect(Collectors.toList());
    }

    public static class BatchId {
        private File exportSet;
        private int bCount;

        public BatchId(File exportSet, int bCount) {
            this.exportSet = exportSet;
            this.bCount = bCount;
        }

        public File getExportSet() {
            return exportSet;
        }

        public void setExportSet(File exportSet) {
            this.exportSet = exportSet;
        }

        public int getCount() {
            return bCount;
        }

        public void setCount(int bCount) {
            this.bCount = bCount;
        }
    }
}

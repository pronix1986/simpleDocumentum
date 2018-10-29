package com.cchis.dctm.util.export.util;

import com.cchis.dctm.util.export.PublishType;
import com.cchis.dctm.util.export.SessionManagerHandler;
import com.cchis.dctm.util.export.WebcHandler;
import com.cchis.dctm.util.export.exception.ExportException;
import com.cchis.dctm.util.export.report.ReportRecord;
import com.documentum.admin.object.IDfWebCacheConfig;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.common.DfException;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.cchis.dctm.util.export.util.ExportConstants.*;

/**
 * Not in use now. Too slow.
 */
public class SlowDqlReportExecutorEx extends ThreadPoolExecutor {
    private static final int CORE_POOL_SIZE = SLOW_CONFIG_EXECUTOR_COUNT;
    private static final int MAX_POOL_SIZE = SLOW_CONFIG_EXECUTOR_COUNT; // Integer.MAX_VALUE;
    private static final long KEEP_ALIVE = 0L;
    private static SlowDqlReportExecutorEx current = null;
    private IDfWebCacheConfig config;
    private Set<String> versions;
    private final Map<String, Future<List<ReportRecord>>> result = Collections.synchronizedMap(new HashMap<>());
    private static final AtomicInteger threadNumber = new AtomicInteger(1);

    private String defaultDQLTemplate;

    public SlowDqlReportExecutorEx(IDfWebCacheConfig config, Set<String> versions) {
        super(CORE_POOL_SIZE, MAX_POOL_SIZE, KEEP_ALIVE, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>() ,
                r -> {
                    String threadName = "SlowDQLThread" + DASH + threadNumber.getAndIncrement();
                    Thread t = new Thread(r, threadName);
                    t.setPriority(Thread.MAX_PRIORITY);
                    return t;
                });

        try {
            this.config = config;
            this.versions = versions;
//            String dql = String.format("select r_object_id, i_vstamp, i_is_replica, r_aspect_name, i_is_reference from %s (all) where folder('%s', descend)",
            String dql = String.format("select r_object_id from %s (all) where folder('%s', descend)",
                    WebcHandler.getPublishType(config),
                    WebcHandler.getPublishFolder(config));
            dql = PublishType.adjustDql(dql, config);
            defaultDQLTemplate = dql + " and any r_version_label = '%s'";
        } catch (DfException e) {
            throw new ExportException("SlowDqlReportExecutorEx()", e);
        }
    }

    public void submit(final String version) {
        final String dql = String.format(defaultDQLTemplate, version);
        Future<List<ReportRecord>> future = submit(() -> {
            List<ReportRecord> result = new ArrayList<>();
            IDfSession session = SessionManagerHandler.getInstance().newSession();
            IDfCollection collection = null;
            try {
                //IDfEnumeration enumeration = session.getObjectsByQuery(dql, WebcHandler.getPublishType(config));
                //while(enumeration.hasMoreElements()) {
                //    ReportRecord record = new ReportRecord((IDfSysObject) enumeration.nextElement());
                collection = Util.runQuery(session, dql);
                while (collection.next()) {
                    ReportRecord record = new ReportRecord(session, collection.getId(DCTM_R_OBJECT_ID));
                    result.add(record);
                }
                return result;
            } catch(DfException e) {
                throw new ExportException("Error during slow DQL execution", e);
            } finally {
                Util.closeCollection(collection);
                SessionManagerHandler.getInstance().releaseSession(session);
            }
        });
        result.put(version, future);
    }

    public static SlowDqlReportExecutorEx createSubmitted(IDfWebCacheConfig config, Set<String> versions) {
        SlowDqlReportExecutorEx executor = new SlowDqlReportExecutorEx(config, versions);
        for (String version : versions) {
            executor.submit(version);
        }
        Util.shutdownExecutorService(current);
        current = executor;
        return executor;
    }

    public synchronized List<ReportRecord> getReportRecords(String key) throws ExecutionException, InterruptedException {
        return result.get(key).get();
    }

    public static SlowDqlReportExecutorEx getCurrent() {
        return current;
    }
}

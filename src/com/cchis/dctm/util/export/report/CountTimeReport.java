package com.cchis.dctm.util.export.report;

import com.cchis.dctm.util.export.ConfigId;
import com.cchis.dctm.util.export.Counter;
import com.cchis.dctm.util.export.PublishType;
import com.cchis.dctm.util.export.WebcHandler;
import com.cchis.dctm.util.export.util.StopWatchEx;
import com.cchis.dctm.util.export.util.Util;
import com.documentum.admin.object.IDfWebCacheConfig;
import org.apache.log4j.Logger;

import java.util.*;

import static com.cchis.dctm.util.export.util.ExportConstants.*;

public class CountTimeReport extends AbstractReport {

    private static final Logger LOG = Logger.getLogger(CountTimeReport.class);

    private static final CountTimeReport INSTANCE = new CountTimeReport();

    public static CountTimeReport getInstance() {
        return INSTANCE;
    }

    private final Map<String, CountTime> configCountTime = new LinkedHashMap<>();
    private final Map<String, CountTime> summaryCountTime = new LinkedHashMap<>();

    private long totalDirectTime = 0L;

    private CountTimeReport() {
        summaryCountTime.put(CHECK_SERVICE_KEY, new CountTime(new CountRecord()));
    }
    @Override
    public synchronized void processReport(IReport iReport) {
        ConfigReport report = (ConfigReport) iReport;
        String configName = report.getcId().getConfigName();
        String bookCode = WebcHandler.getBookCode(configName).name();
        AbstractCount count = report.getCountRecord();
        configCountTime.put(configName, new CountTime(count));
        summaryCountTime.merge(bookCode, new CountTime(AbstractCount.copyOf(count)), CountTime::sum);
    }

    public void addCheckServicesTime(final long time) {
        if (!PublishType.launchBookAsync()) {
            summaryCountTime.merge(CHECK_SERVICE_KEY, new CountTime(time), CountTime::sum);
        }
    }

    private void addTime(String configName, long time) {
        if (!PublishType.launchBookAsync()) {
            String bookCode = WebcHandler.getBookCode(configName).name();
            configCountTime.merge(configName, new CountTime(time), CountTime::sum);
            summaryCountTime.merge(bookCode, new CountTime(time), CountTime::sum);
        }
    }

    public void addTime(String configName, Counter counter) {
        addTime(configName, counter.getStopWatch());
    }

    public void addTime(String configName, StopWatchEx stopWatch) {
        long time = stopWatch.getTimeAndRestart();
        LOG.debug(String.format("Config %s publish time: %s ms", configName, time));
        addTime(configName, time);
    }

    public void addCheckServicesTime(Counter counter) {
        addCheckServicesTime(counter.getStopWatch().getTimeAndRestart());
    }

    public void addTotalDirectTime(long totalDirectTime) {
        this.totalDirectTime = totalDirectTime;
    }

    @Override
    public List<String> getReportLines() {
        List<String> lines = new ArrayList<>();
        if (Util.isEmptyCollection(summaryCountTime.keySet())) {
            return lines;
        }
        configCountTime.forEach((configName, countTime) ->
            lines.add(Util.commaJoinStrings(configName, countTime.getCount(), countTime.getTime())));

        Collections.sort(lines);
        lines.add(Util.wrap(TOTAL, " --- "));
        final CountTime totalCountTime = new CountTime();
        summaryCountTime.forEach((bookCode, countTime) -> {
            totalCountTime.sum(countTime);
            lines.add(Util.commaJoinStrings(bookCode, countTime.getCount(), countTime.getTime()));
        });
        if (totalDirectTime > 0) {
            totalCountTime.setTime(totalDirectTime);
        }
        lines.add(Util.commaJoinStrings(TOTAL, totalCountTime.getCount(), totalCountTime.getTime()));
        lines.add(0, getHeader());
        return lines;
    }

    private String getHeader() {
        return Util.commaJoinStrings("Name",
                Util.simpleArrayToStringAsCSV(CountRecord.getHeaders().toString()),
                "TimeTaken");
    }

    static class CountTime {
        private AbstractCount count;
        private Long time;

        CountTime() {
            this(null);
        }

        CountTime(AbstractCount count) {
            this(count, 0L);
        }

        CountTime(long time) {
            this(null, time);
        }

        CountTime(AbstractCount count, long time) {
            this.count = count;
            this.time = time;
        }

        AbstractCount getCount() {
            return count;
        }

        void setCount(AbstractCount count) {
            this.count = count;
        }

        long getTime() {
            return time;
        }

        void setTime(long time) {
            this.time = time;
        }

        CountTime sum(CountTime anotherCountTime) {
            AbstractCount anotherCount = anotherCountTime.getCount();
            if (count == null) {
                count = AbstractCount.copyOf(anotherCount);
            } else {
                count.sum(anotherCountTime.getCount());
            }
            time += anotherCountTime.getTime();
            return this;
        }
    }
}

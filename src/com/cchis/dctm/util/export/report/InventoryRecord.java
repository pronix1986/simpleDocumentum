package com.cchis.dctm.util.export.report;

import com.cchis.dctm.util.export.util.Util;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

import static com.cchis.dctm.util.export.util.ExportConstants.*;

class InventoryRecord extends ReportRecord {

    protected static final ReportRecord DUMMY_RECORD =
            new ReportRecord((Util.commaJoinStrings(DCTM_I_CHRONICLE_ID, "Counts")),
                    DCTM_R_OBJECT_ID, DCTM_OBJECT_NAME, DCTM_R_FOLDER_PATH,
                    Util.commaJoinStrings(VERSION_LABELS, "Approved?", "CURRENT?"),
                    DCTM_R_CURRENT_STATE, null);

    private String chronicleId = EMPTY_STRING;
    private final List<ReportRecord> reportRecords = new ArrayList<>();

    protected InventoryRecord() { }

    InventoryRecord (ReportRecord report) {
        add(report);
    }

    private void add(ReportRecord reportRecord) {
        this.chronicleId = reportRecord.getChronicleId();
        reportRecords.add(reportRecord);
    }



    public List<ReportRecord> getReportRecords() {
        return reportRecords;
    }

    public int getCounts() {
        return reportRecords.size();
    }

    public String getChronicleId() {
        return chronicleId;
    }

    public void append(InventoryRecord ir) {
        if (chronicleId.equalsIgnoreCase(ir.getChronicleId())) {
            reportRecords.addAll(ir.getReportRecords());
        }
    }

    public static InventoryRecord combineRecords(List<InventoryRecord> irs) {
        if (Util.isNotEmptyCollection(irs)) {
            InventoryRecord first = irs.get(0);
            irs.stream().skip(1).forEach(first::append);
            return first;
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InventoryRecord that = (InventoryRecord) o;
        return (chronicleId != null ? chronicleId.equals(that.chronicleId) : that.chronicleId == null);
    }

    @Override
    public int hashCode() {
        return chronicleId != null ? chronicleId.hashCode() : 0;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(chronicleId + COMMA + getCounts() + StringUtils.repeat(COMMA, 6) + NEW_LINE);
        for (int i = 0; i < reportRecords.size(); i++) {
            ReportRecord rr = reportRecords.get(i);
            builder.append(Util.commaJoinStrings(EMPTY_STRING, EMPTY_STRING,
                    rr.getId(), Util.escapeCommaCSV(rr.getFilteredPathNullSafe()), rr.getVersion(),
                            rr.isApproved(), rr.isCurrent(), rr.getCurrentState()));
            if(i != reportRecords.size() - 1) {
                builder.append(NEW_LINE);
            }
        }
        return builder.toString();
    }
}

class InventoryCountRecord extends InventoryRecord {

    private final int count;

    InventoryCountRecord(int count) {
        this.count = count;
    }

    @Override
    public int getCounts() {
        return super.getCounts();
    }

    @Override
    public String toString() {
        return TOTAL + COMMA + count + StringUtils.repeat(COMMA, 6);
    }
}

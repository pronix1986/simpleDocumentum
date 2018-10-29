package com.cchis.dctm.util.export.util.exec;

import com.cchis.dctm.util.export.DocumentHandler;
import com.cchis.dctm.util.export.SessionManagerHandler;
import com.cchis.dctm.util.export.SqlConnector;
import com.cchis.dctm.util.export.util.Util;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfId;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.cchis.dctm.util.export.util.ExportConstants.MSG_ERROR;
import static com.cchis.dctm.util.export.util.ExportConstants.NEW_LINE;
import static com.cchis.dctm.util.export.util.exec.LongFieldsValidator.ICS_INSIGHT_REG;
import static com.cchis.dctm.util.export.util.exec.LongFieldsValidator.ICS_INSOURCE;

public class LongFieldsChecker {
    private static final Logger LOG = Logger.getLogger(LongFieldsChecker.class);

    private static final String DCTM_ICS_REG_NAME = "ics_reg_name";
    private static final String DCTM_ICS_CURRENT_CITATION = "ics_current_citation";

    private static Connection conn;

    public static void main(String[] args) {
        LOG.info("-- LongFieldsChecker --");
        conn = SqlConnector.getInstance().getSqlConnection();
        new LongFieldsChecker().check();
        SqlConnector.getInstance().closeSqlConnection();
    }

    private void check() {
        IDfSession session = SessionManagerHandler.getInstance().getMainSession();


        final Map<String, String> map = new HashMap<>();
        map.put(ICS_INSIGHT_REG, DCTM_ICS_REG_NAME);
        map.put(ICS_INSOURCE, DCTM_ICS_CURRENT_CITATION);

        try (final Statement statement = conn.createStatement()) {
            for(Map.Entry<String, String> entry:  map.entrySet()) {
                String type = entry.getKey();
                String attr = entry.getValue();

                List<String> ids = new ArrayList<>();
                List<IDfSysObject> docs = new ArrayList<>();

                String query = String.format("select r_object_id from %s_sp where len(%s) > 70", type, attr);
                try (ResultSet rs0 = statement.executeQuery(query)) {
                    while (rs0.next()) {
                        String id  = rs0.getString(1);
                        ids.add(id);
                    }
                }
                for (String id: ids) {
                    try {
                        IDfSysObject doc = (IDfSysObject) session.getObject(new DfId(id));
                        if (DocumentHandler.isReadyToImport(doc)) {
                            docs.add(doc);
                        }

                    } catch (Exception e) {
                        LOG.warn(MSG_ERROR, e);
                    }
                }
                LOG.info("Ready to import doc count with long " + attr + ": " + docs.size());
                StringBuilder builder = new StringBuilder();
                docs.forEach(doc -> {
                    try {
                        builder.append(Util.commaJoinStrings(doc.getObjectId().getId(), DocumentHandler.getFolderName(doc),
                                Util.escapeCommaCSV(doc.getObjectName()), Util.escapeCommaCSV(doc.getString(attr))));
                        builder.append(NEW_LINE);
                    } catch (DfException e) {
                        LOG.warn(MSG_ERROR, e);
                    }
                });
                LOG.info(builder.toString());
            }

        } catch (Exception e) {
            LOG.warn(MSG_ERROR, e);
        }
    }
}

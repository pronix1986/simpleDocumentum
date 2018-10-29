package com.cchis.dctm.util.export.util.exec;

import com.cchis.dctm.util.export.LogHandler;
import com.cchis.dctm.util.export.SessionManagerHandler;
import com.cchis.dctm.util.export.SqlConnector;
import com.cchis.dctm.util.export.util.FileUtil;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.common.DfId;
import org.apache.log4j.Logger;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.cchis.dctm.util.export.util.ExportConstants.*;

public class CRLFIssueHelper {
    private static final Logger LOG = Logger.getLogger(CRLFIssueHelper.class);

    private static Connection conn;

    public static void main(String[] args) {
        try {
            String id = "0900496281a0faba";

            IDfSession session = SessionManagerHandler.getInstance().getMainSession();
            IDfSysObject object = (IDfSysObject) session.getObject(new DfId(id));

            String origDescription = object.getString("ics_description");

            conn = SqlConnector.getInstance().getSqlConnection();

            String scsDescription = getSCSDescription(id);

            List<String> descs = new ArrayList<>(2);
            descs.add(origDescription);
            descs.add(scsDescription);

            String fileName = id + UNDERSCORE + "descriptions.txt";
            File output = new File(REPORT_FOLDER_PATH + File.separator + fileName);

            FileUtil.writeLinesSilently(output, descs);

            SqlConnector.getInstance().closeSqlConnection();

        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
        }


    }

    private static String getSCSDescription(String id) {
        String scsDescription = null;
        try (final Statement statement = conn.createStatement()) {

            String query = String.format("select ics_description FROM [DCTM_SCS_Dev].[dbo].[AK_INSIGHTREGS_APPROVED_s] where r_object_id = '%s'", id);
            try (ResultSet rs0 = statement.executeQuery(query)) {
                while (rs0.next()) {
                    scsDescription = rs0.getString(1);
                }
            }

        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
        }
        return scsDescription;
    }
}

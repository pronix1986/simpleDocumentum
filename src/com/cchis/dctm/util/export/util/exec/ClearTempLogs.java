package com.cchis.dctm.util.export.util.exec;

import com.cchis.dctm.util.export.DocumentHandler;
import com.cchis.dctm.util.export.SessionManagerHandler;

import static com.cchis.dctm.util.export.util.ExportConstants.DQL_SYSADMIN_SCS_LOGS_FOR_NAME;

public class ClearTempLogs {
    public static void main(String[] args) {
        String configNamePrefix = "TEMP-";
        String dql = String.format(DQL_SYSADMIN_SCS_LOGS_FOR_NAME, configNamePrefix);
        // Do not use config session. Concurrency issue.
        DocumentHandler.destroySysObjects(SessionManagerHandler.getInstance().getCleanupSession(), dql);
    }
}

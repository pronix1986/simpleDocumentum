package com.cchis.dctm.util.export.util.exec;

import com.cchis.dctm.util.export.*;
import com.documentum.admin.object.IDfWebCacheConfig;
import com.documentum.fc.client.IDfEnumeration;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSysObject;
import org.apache.log4j.Logger;


import static com.cchis.dctm.util.export.util.ExportConstants.*;

public class FieldAdder {
    private static final Logger LOG = Logger.getLogger(FieldAdder.class);

    public static void main(String[] args) {
        LOG.info("-- FieldAdder --");
        IDfSession session = SessionManagerHandler.getInstance().getMainSession();

        String dql = "select r_object_id, i_vstamp, i_is_replica, r_aspect_name, i_is_reference from dm_webc_config where object_name like 'CO%' or object_name like 'LA%' or object_name like 'MO%' and object_name not like '%IMAGE%' order by object_name";
        /*String dql = "select r_object_id, i_vstamp, i_is_replica, r_aspect_name, i_is_reference from dm_webc_config " +
                "where object_name like 'CO-INSIGHT-BILL%'";*/

        try {
            IDfSysObject scsConfig = (IDfSysObject) session.getObjectByQualification(DQL_QUAL_SCS_CONFIG);
            scsConfig.setRepeatingString(ATTR_EXTRA_ARGS, 0, "sync_on_zero_updates TRUE");
            LOG.info(MSG_SCS_EXTRA_ARGS_SET);
            scsConfig.save();

        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
        }

        try {
            final IDfEnumeration enumeration = session.getObjectsByQuery(dql, DCTM_WEBC_CONFIG);

            while (enumeration.hasMoreElements()) {
                try {
                    IDfWebCacheConfig config = (IDfWebCacheConfig) enumeration.nextElement();
                    WebcHandler.appendWebcAttr(config, new String[] {"cchis_records.ics_silent_basic", BOOLEAN_TYPE, ZERO, FALSE_SYM});
                    WebcHandler.appendWebcAttr(config, new String[] {"cchis_records.ics_silent_enhanced", BOOLEAN_TYPE, ZERO, FALSE_SYM});
                    LOG.info("new fields added");

                    PublishResult result = new PublishResult(config);
                    WebcHandler.publishSCSConfig(session, result, WebcHandler.PublishingOption.UPDATE);

                    String info = result.getConfigName() + " | " + result.getWebcCompletedStatus();
                    LOG.info(info);

                } catch (Exception dfe) {
                    LogHandler.logWithDetails(LOG, MSG_ERROR, dfe);
                }

            }

        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
        } finally {
            // TODO:
        }

        try {
            IDfSysObject scsConfig = (IDfSysObject) session.getObjectByQualification(DQL_QUAL_SCS_CONFIG);
            scsConfig.removeAll(ATTR_EXTRA_ARGS);
            LOG.info(MSG_SCS_EXTRA_ARGS_UNSET);
            scsConfig.save();

        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
        }
    }
}

package com.cchis.dctm.util.export.util.exec;

import com.cchis.dctm.util.export.LogHandler;
import com.cchis.dctm.util.export.SessionManagerHandler;
import com.documentum.admin.object.IDfWebCacheConfig;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.common.DfId;
import org.apache.log4j.Logger;

import static com.cchis.dctm.util.export.util.ExportConstants.MSG_ERROR;

public class QuickSIPublish {
    private static final Logger LOG = Logger.getLogger(QuickSIPublish.class);
    public static void main(String[] args) {
        LOG.info("-- QuickSAPublish --");
        try {
            String configId = "08004962805c8d02";
            IDfSession session = SessionManagerHandler.getInstance().getMainSession();
            IDfWebCacheConfig config = (IDfWebCacheConfig) session.getObject(new DfId(configId));

            

/*            String configId = "08004962805c8d02";
            List<String> ids = new ArrayList<>();
            ids.add("0900496281a0efd4");
            ids.add("0900496281a0efcf");
            ids.add("0900496281a0db15");
            ids.add("0900496281a0db11");
            ids.add("0900496281a09b24");
            ids.add("0900496281a09ad5");
            ids.add("0900496281a09ab1");
            ids.add("0900496281a0350b");
            ids.add("09004962819fcc8b");
            ids.add("09004962819f9933");
            ids.add("09004962819f787c");
            ids.add("09004962819f786f");
            ids.add("09004962819f31b5");
            ids.add("09004962819e4fe0");
            ids.add("09004962819e28e9");
            IDfSession session = SessionManagerHandler.getInstance().getMainSession();
            IDfWebCacheConfig config = (IDfWebCacheConfig) session.getObject(new DfId(configId));
            PublishResult result = new PublishResult(config, null);
            //WebcHandler.republishSCSConfig(session, result, ids, WebcHandler.PublishingOption.SINGLE_ITEM);
            WebcHandler.republishSCSConfigSingleItem(session, result, ids);

            Util.sleepSilently(DEFAULT_INTERVAL);
            LOG.info(result.getWebcCompletedStatus());*/
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
        }




    }
}

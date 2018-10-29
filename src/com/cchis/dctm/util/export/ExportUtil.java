package com.cchis.dctm.util.export;

import com.cchis.dctm.util.export.cli.CLIOption;
import com.cchis.dctm.util.export.cli.CLIParser;
import com.cchis.dctm.util.export.util.Util;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.log4j.Logger;

import java.util.*;

import static com.cchis.dctm.util.export.util.ExportConstants.*;

public final class ExportUtil {

    private static final Logger LOG = Logger.getLogger(ExportUtil.class);

    private ExportUtil() { }

    static { // Time for reports.
        PropertiesHolder properties = PropertiesHolder.getInstance();
        properties.setProperty(PROP_START_DATE_TIME, DateFormatUtils.format(new Date(), DATE_TIME_FORMAT));
    }

    public static void main(String[] args) {
        SessionManagerHandler sessionManagerHandler = SessionManagerHandler.getInstance();
        GlobalInitializer initializer = GlobalInitializer.getInstance();
        try {
            LOG.info("-- Export Util --");
            CLIOption option = new CLIParser(args).parse();
            LOG.trace(String.format("%s%n%s%n%s", option.getPublishType(), option.getDql(), option.getDates()));

            initializer.init();
            WebcHandler.republishSCSConfigsAllVersions();
            Util.sleepSilently(END_DELAY);

        } catch (Exception e) {
            LOG.error(MSG_ERROR, e);
        } finally {
            initializer.toDefault();
            if (sessionManagerHandler != null) sessionManagerHandler.releaseMajorSessions();
            LOG.info("Fin");
        }
    }

}
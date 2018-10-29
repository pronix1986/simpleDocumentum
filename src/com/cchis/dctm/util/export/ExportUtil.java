package com.cchis.dctm.util.export;

import com.cchis.dctm.util.export.cli.CLIOption;
import com.cchis.dctm.util.export.cli.CLIParser;
import com.cchis.dctm.util.export.exception.ExportException;
import com.cchis.dctm.util.export.report.BookStartDateChecker;
import com.cchis.dctm.util.export.report.ConfigReport;
import com.cchis.dctm.util.export.report.ReportRecord;
import com.cchis.dctm.util.export.util.DuplicateNamesZipUpdater;
import com.cchis.dctm.util.export.util.FileUtil;
import com.cchis.dctm.util.export.util.Util;
import com.documentum.admin.object.IDfWebCacheConfig;
import com.documentum.admin.object.IDfWebCacheTarget;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.client.impl.connection.docbase.ClientInfo;
import com.documentum.fc.client.impl.docbase.DocbaseDateFormat;
import com.documentum.fc.client.impl.session.ISessionManager;
import com.documentum.fc.common.DfId;
import com.documentum.fc.common.DfPreferences;
import com.documentum.fc.common.IDfTime;
import com.documentum.fc.common.impl.preferences.PreferencesManager;
//import com.documentum.server.impl.method.fulltext.startup.FTIndexAgentBoot;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

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

            //throw new ExportException("test");
        } catch (Exception e) {
            LOG.error(MSG_ERROR, e);
        } finally {
            initializer.toDefault();
            if (sessionManagerHandler != null) sessionManagerHandler.releaseMajorSessions();
            LOG.info("Fin");
        }
    }

}
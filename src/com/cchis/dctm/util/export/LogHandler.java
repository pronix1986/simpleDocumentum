package com.cchis.dctm.util.export;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import static com.cchis.dctm.util.export.util.ExportConstants.EMPTY_STRING;

public final class LogHandler {
    private static final String DETAILS_IN_LOG = " See details in log file. ";
    private static final String DETAILS = " Details:";

    public static void logWithDetails(Logger logger, Level mainLevel, Level detailsLevel, String message, String postMessage, Throwable t) {
        logger.log(mainLevel, (message + DETAILS_IN_LOG + (StringUtils.isNotEmpty(postMessage)?postMessage:EMPTY_STRING)));
        logger.log(detailsLevel, (message + DETAILS), t);
    }

    public static void logWithDetails(Logger logger, Level mainLevel, String message, Throwable t) {
        logWithDetails(logger, mainLevel, Level.DEBUG, message, null, t);
    }

    public static void logWithDetails(Logger logger, String message, Throwable t) {
        logWithDetails(logger, Level.ERROR, message, t);
    }
}

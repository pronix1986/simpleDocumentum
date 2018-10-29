package com.cchis.dctm.util.export;

import com.cchis.dctm.util.export.cli.CLIOption;
import com.cchis.dctm.util.export.util.NaiveBookCache;
import com.cchis.dctm.util.export.util.Util;
import com.documentum.admin.object.IDfWebCacheConfig;
import com.documentum.fc.common.DfException;
import org.apache.commons.lang.StringUtils;
import org.omg.CORBA.UNKNOWN;

import static com.cchis.dctm.util.export.util.ExportConstants.*;
import static com.cchis.dctm.util.export.util.ExportConstants.DQL_MODIFIED_DATETO_POSTFIX;

public enum PublishType {
    FULL, DELTA, UNKNOWN;

    public static final String WEBC_SUCCESS = "Completed Successfully";
    public static final String WEBC_APPLY_TARGET = "Apply Target Synchronization";
    public static final String WEBC_BEGIN = "Begin Publish Operation";
    private static final int PUBLISH_METHOD;
    private static final boolean DELAY_SLOW_CONFIGS;
    private static final boolean SINGLE_SESSION;
    private static final boolean TARGET_SYNC_DISABLED;
    private static final boolean CHECK_SERVICES;
    private static final boolean PUBLISH_EXPORTSET_ONLY;
    private static final boolean DELTA_CREATE_NEW_CONFIG;
    private static final NaiveBookCache BOOK_CACHE = NaiveBookCache.getInstance();

    static {
        PropertiesHolder properties = PropertiesHolder.getInstance();
        String publishMethodStr = properties.getProperty(PROP_PUBLISH_METHOD);
        PUBLISH_METHOD = StringUtils.isEmpty(publishMethodStr)
                ? DEFAULT_PUBLISH_METHOD : Integer.parseInt(publishMethodStr);
        DELAY_SLOW_CONFIGS = Boolean.parseBoolean(properties.getProperty(PROP_DELAY_SLOW_CONFIGS));
        String singleSessionStr = properties.getProperty(PROP_SINGLE_SESSION);
        SINGLE_SESSION = (StringUtils.isEmpty(singleSessionStr)) ? DEFAUL_SINGLE_OPTION : Boolean.parseBoolean(singleSessionStr);
        TARGET_SYNC_DISABLED = Boolean.parseBoolean(properties.getProperty(PROP_TARGET_SYNC_DISABLED));
        CHECK_SERVICES = Boolean.parseBoolean(properties.getProperty(PROP_CHECK_SERVICES));
        PUBLISH_EXPORTSET_ONLY = Boolean.parseBoolean(properties.getProperty(PROP_PUBLISH_EXPORTSET_ONLY));
        DELTA_CREATE_NEW_CONFIG = Boolean.parseBoolean(properties.getProperty(PROP_DELTA_CREATE_NEW_CONFIG));
    }

    public static PublishType getCurrent() {
        CLIOption current = CLIOption.getCurrent();
        return current != null ? current.getPublishType() : PublishType.UNKNOWN;
    }

    public static boolean shouldCreateNewConfig() {
        switch (getCurrent()) {
            case DELTA: return DELTA_CREATE_NEW_CONFIG;
            case FULL: return true;
            default: return true;
        }
    }

    public static boolean shouldCheckServices() {
        switch (getCurrent()) {
            case DELTA: return false;
            case FULL: return CHECK_SERVICES;
            default: return true;
        }
    }

    public static boolean prefLaunchAsync() {
        switch (getCurrent()) {
            case DELTA: return DELTA_CREATE_NEW_CONFIG;
            case FULL: return true;
            default: return true;
        }
    }

    public static boolean launchBookAsync() {
        return prefLaunchAsync() && LAUNCH_BOOK_ASYNC;
    }

    public static int getPublishMethod() {
        if (!USE_PUBLISHING) return PUBLISH_FREE_METHOD;
        switch (getCurrent()) {
            case DELTA: return SINGLE_ITEM_PUBLISH_METHOD;
            case FULL: return PUBLISH_METHOD;
            default: return DEFAULT_PUBLISH_METHOD;
        }
    }

    public static boolean delaySlowConfigs() {
        switch (getCurrent()) {
            case DELTA: return false;
            case FULL: return DELAY_SLOW_CONFIGS && USE_PUBLISHING;
            default: return false;
        }
    }

    public static boolean isSingleSession() {
        switch (getCurrent()) {
            case DELTA: return !DELTA_CREATE_NEW_CONFIG && SINGLE_SESSION;
            case FULL: return SINGLE_SESSION;
            default: return DEFAUL_SINGLE_OPTION;
        }
    }

    public static String getWebcCompletedStatus() {
        switch (getCurrent()) {
            case DELTA: {
                if (DELTA_CREATE_NEW_CONFIG && TARGET_SYNC_DISABLED) {
                    return WEBC_APPLY_TARGET;
                } else {
                    return WEBC_SUCCESS;
                }
            }
            case FULL: {
                if(PUBLISH_EXPORTSET_ONLY) return WEBC_BEGIN;
                return TARGET_SYNC_DISABLED ? WEBC_APPLY_TARGET : WEBC_SUCCESS;
            }
            default: return WEBC_SUCCESS;
        }
    }

    public static boolean shouldGetExportSetOnly() {
        switch (getCurrent()) {
            case DELTA: return false;
            case FULL: return PUBLISH_EXPORTSET_ONLY;
            default: return false;
        }
    }

    public static String getDql() {
        return CLIOption.getCurrent().getDql();
    }

    public static String getCombDate(String configName) {
        return CLIOption.getCurrent().getDates(configName);
    }

    public static String[] getModifiedDates(String configName) {
        String combDate = getCombDate(configName);
        if (StringUtils.isNotEmpty(combDate)) {
            return Util.getFromToDates(combDate);
        }
        if (USE_BOOK_CACHE) BOOK_CACHE.put(configName, CACHE_COMB_DATES_EMPTY, Boolean.TRUE);
        return new String[2];
    }

    public static String adjustDql(String dql, IDfWebCacheConfig config) throws DfException {
        String configName = EMPTY_STRING;
        if (USE_BOOK_CACHE) {
            configName = ConfigId.getConfigIdFailSafe(config).getConfigName();
            Boolean skip = (Boolean) BOOK_CACHE.get(configName, CACHE_COMB_DATES_EMPTY);
            if (skip != null && skip) {
                return dql;
            }
        }
        String newDql = dql;
        String[] fromToDates = PublishType.getModifiedDates(WebcHandler.getParentConfigName(config.getObjectName()));
        String dateFrom = fromToDates[0];
        String dateTo = fromToDates[1];

        if (StringUtils.isNotEmpty(dateFrom)) {
            newDql += String.format(DQL_MODIFIED_DATEFROM_POSTFIX, dateFrom);
        }
        if (StringUtils.isNotEmpty(dateTo)) {
            newDql += String.format(DQL_MODIFIED_DATETO_POSTFIX, dateTo);
        }
        if (USE_BOOK_CACHE && dql.equals(newDql)) {
            BOOK_CACHE.put(configName, CACHE_COMB_DATES_EMPTY, Boolean.TRUE);
        }
        return newDql;
    }

}

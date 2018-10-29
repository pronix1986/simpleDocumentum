package com.cchis.dctm.util.export.util;

import com.cchis.dctm.util.export.PropertiesHolder;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public final class ExportConstants {

    private ExportConstants() { }

	public static final String CURRENT_YEAR = "2018-05-15";
    public static final String PROP_DB_USER_NAME = "db.user.name";
    public static final String PROP_DB_USER_PSWD = "db.user.password";
    public static final String PROP_DB_URL = "db.url";
    public static final String PROP_DB_SCS_NAME = "db.scs.name";
    public static final String PROP_SCS_TARGET_HOST_1 = "scs.target.host.1";
    public static final String PROP_SCS_TARGET_HOST_2 = "scs.target.host.2";
    public static final String PROP_SCS_SOURCE_HOST_1 = "scs.source.host.1";
    public static final String PROP_SCS_SOURCE_HOST_2 = "scs.source.host.2";
    public static final String PROP_SCS_TARGET_SHARE = "scs.target.share";
    public static final String PROP_CHECK_SERVICES = "check.services";
    public static final String PROP_PUBLISH_EXPORTSET_ONLY = "publish.exportset.only";

    public static final String PROP_ENV_PROPERTIES = "/env.properties";
    public static final String PROP_EXPORT_PROPERTIES = "/export.properties";
    public static final String PROP_SUPERUSER_NAME = "superuser.name";
    public static final String PROP_SUPERUSER_PSWD = "superuser.password";
    public static final String PROP_DOCBASE_NAME = "docbase.name";
    public static final String PROP_EXPORT_DIR = "export.dir";
    public static final String PROP_TARGET_ROOT = "target.root.dir";
    public static final String PROP_BATCH_TEMP_DIR = "batch.temp.dir";
    public static final String PROP_PUBLISH_LOG_LEVEL = "publish.log.level";

    public static final String PROP_PUBLISH_METHOD = "publish.method";
    public static final String PROP_SINGLE_SESSION = "single.session";
    public static final String PROP_EXECUTOR_COUNT = "executor.count";
    public static final String PROP_PUBLISH_TIMEOUT = "publish.timeout";
    public static final String PROP_WEBC_LOCK_TIMEOUT = "webc.lock.timeout";
    public static final String PROP_JOB_TIMEOUT = "job.timeout";
    public static final String PROP_STATUS_INTERVAL = "status.check.interval";
    public static final String PROP_LOCK_INTERVAL = "lock.check.interval";
    public static final String PROP_DELAY_SLOW_CONFIGS = "delay.slow.configs";
    public static final String PROP_DETAILED_REPORT = "detailed.report";
    public static final String PROP_RETRY_IF_ERROR = "retry.if.error";
    public static final String PROP_TARGET_SYNC_DISABLED = "target.sync.disabled";
    public static final String PROP_PRE_SYNC_SCRIPT = "pre.sync.script";
    public static final String PROP_CREATE_INDEXES = "create.indexes";
    public static final String PROP_SET_SCS_EXTRA_ARGS = "set.scs.extra.args";
    public static final String PROP_DELTA_CREATE_NEW_CONFIG = "delta.create.new.config";

    public static final String PROP_REPORT_DIR = "report.dir";
    public static final String PROP_START_DATE_TIME = "start.date.time";
    public static final String PROP_PROCESS_SLOW_CONFIGS = "process.slow.configs";
    public static final String PROP_END = "config.processed";

    public static final String PROP_USE_PUBLISHING = "use.publishing";
    public static final String PROP_GLOBAL_INIT = "global.init";
    public static final String PROP_USE_BOOK_CACHE = "use.book.cache";
    public static final String PROP_LAUNCH_BOOK_ASYNC = "launch.book.async";

    public static final String CACHE_WEBC_TYPE = "webc.type";
    public static final String CACHE_WEBC_FORMAT = "webc.format";
    public static final String CACHE_PUBLISH_FOLDER = "publish.folder";
    public static final String CACHE_COMB_DATES_EMPTY = "comb.date.empty";

    public static final String CHECK_SERVICE_KEY = "Check_Services";
    public static final String APPROVED_LABEL = "Approved";
    public static final String APPROVED_POSTFIX = "-APPROVED";
    public static final String CURRENT_LABEL = "CURRENT";
    public static final String TOTAL = "Total";
    public static final String TOTAL_TIME = "Total Time";
    public static final String FORMAT = "Format";
    public static final String VERSION_LABELS = "Version Labels";
    public static final String MISSED_CONFIG_TITLE = "MISSED-CONFIG";
    public static final String ROOT_IMAGE_CONFIG_NAME = "IMAGES-APPROVED";
    public static final String ROOT_IMAGE_PATH = "/icspipeline.com/Images";


    public static final String SQL_DATE_FORMAT = "yyyy-MM-dd";
    public static final String SQL_DATE_REGEX = "^\\d{4}-\\d{2}-\\d{2}$";
    public static final String SQL_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";
    public static final String SQL_DATETIME_REGEX = "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}$";
    public static final String DATE_TIME_FORMAT = "yyyy-MM-dd_HH_mm";

    public static final String UTF_8 = CharEncoding.UTF_8;
    public static final Charset UTF_8_CS = StandardCharsets.UTF_8;
    public static final String PROPERTIES_XML = "properties.xml";


    public static final String DQL_HINT_UNCOMMITTED_READ = "UNCOMMITTED_READ";
    public static final String DQL_HINT_SQL_DEF_RESULT_SETS = "SQL_DEF_RESULT_SETS";
    public static final String DQL_QUAL_SCS_CONFIG = "scs_admin_config enable (return_top 1)";
    public static final String DQL_QUAL_DM_WEBPUBLISH_METHOD = "dm_method WHERE object_name = 'dm_webcache_publish'";

    public static final String DQL_SYSADMIN_SCS_LOGS = "SELECT DISTINCT r_object_id FROM dm_document (ALL) WHERE FOLDER('/System/Sysadmin/Reports/WebCache')";
    public static final String DQL_SYSADMIN_SCS_LOGS_INCLUDE = " and object_name like '%%%s%%'";
    public static final String DQL_SYSADMIN_SCS_LOGS_EXCLUDE = " and object_name not like '%%%s%%'";
    public static final String DQL_SYSADMIN_SCS_LOGS_FOR_NAME = DQL_SYSADMIN_SCS_LOGS + DQL_SYSADMIN_SCS_LOGS_INCLUDE;
    public static final String DQL_SYSADMIN_SCS_LOGS_POSTFIX = " order by r_creation_date desc enable (return_top 1)";
    public static final String DQL_QUAL_TOP_SCS_LOG = "dm_document (all) WHERE FOLDER('/System/Sysadmin/Reports/WebCache')" + DQL_SYSADMIN_SCS_LOGS_INCLUDE + DQL_SYSADMIN_SCS_LOGS_POSTFIX;

    public static final String DQL_GET_ALL_SCS_CONFIGS_PREFIX = "SELECT r_object_id, i_vstamp, i_is_replica, r_aspect_name, i_is_reference FROM dm_webc_config WHERE (";
    public static final String DQL_GET_ALL_SCS_CONFIGS_POSTFIX = " ) AND (r_creation_date < '" + CURRENT_YEAR + "' OR title = '" + MISSED_CONFIG_TITLE + "' OR title ='DO_NOT_REMOVE') ORDER BY object_name";
    public static final String DQL_GET_ALL_SCS_CONFIGS_NAME_PATTERN = " object_name LIKE '%s%%'";
    public static final String DQL_GET_ALL_SCS_CONFIGS_EXCLUDE_NAME_PATTERN = " object_name NOT LIKE '%s%%'";
    public static final String DQL_GET_ALL_SCS_CONFIGS_WRONG_VERSION_LABEL = "SELECT r_object_id, i_vstamp, i_is_replica, r_aspect_name, i_is_reference FROM dm_webc_config WHERE (r_creation_date < '" + CURRENT_YEAR + "' AND any version_labels != '" + APPROVED_LABEL + "') ORDER BY object_name";
    public static final String DQL_GET_ZERO_SCS_CONFIGS = DQL_GET_ALL_SCS_CONFIGS_PREFIX + "1 != 1" + DQL_GET_ALL_SCS_CONFIGS_POSTFIX;

    public static final String DQL_QUAL_GET_TEMPLATE_IMAGE_CONFIG = "dm_webc_config where object_name like '%-INSOURCE-IMAGES-%' AND (r_creation_date < '" + CURRENT_YEAR + "') enable (return_top 1)";


    public static final String DQL_GET_ROOT_IMAGE_ID = "select r_object_id from dm_folder where any r_folder_path = '/icspipeline.com/Images'";
    public static final String DQL_QUAL_GET_ROOT_IMAGE_ID = "dm_folder where any r_folder_path = '/icspipeline.com/Images'";

    public static final String DQL_QUAL_GET_FOLDER_ID = "dm_folder where any r_folder_path = '%s'";

    public static final String DQL_DROP_ROOT_IMAGE = "select r_object_id, i_vstamp, i_is_replica, r_aspect_name, i_is_reference from dm_webc_config where object_name = '%s'";

    public static final String DQL_STALE_CONFIGS = "select r_object_id, i_vstamp, i_is_replica, r_aspect_name, i_is_reference from dm_webc_config where (object_name like 'TEMP%' or title = '" + MISSED_CONFIG_TITLE + "' or r_creation_date > '" + CURRENT_YEAR + "') and title != 'DO_NOT_REMOVE'";
    public static final String DQL_STALE_TARGETS = "select r_object_id from dm_webc_target_sp where r_object_id not in (select distinct target_id from dm_webc_config_rp where target_id is not null)";

    public static final String DQL_STALE_JOBS = "select r_object_id, i_vstamp, i_is_replica, r_aspect_name, i_is_reference from dm_job where (method_name = 'dm_webcache_publish' and subject like '%TEMP%' and r_creation_date > '" + CURRENT_YEAR + "')";


    public static final String DQL_GET_PUBLISH_JOB_BY_CONFIG_ID = "dm_job where any method_arguments = '-config_object_id %s'";
    public static final String WEB_PUBLISH_JOB_SUBJECT = "Web publishing method ";

    public static final String DQL_GET_WEBC_REG_TABLES_PREFIX = "SELECT r_object_id FROM dm_registered WHERE ";
    public static final String DQL_GET_WEBC_REG_TABLES_POSTFIX = "";
    public static final String DQL_GET_WEBC_REG_TABLES_NAME_PATTERN = " object_name LIKE '%s%%'";

    public static final String DQL_GET_WEBCLOCK_COUNT_FOR_NAME = "select count(*) as c from dbo.webc_lock_s where object_name = '%s'";
    public static final String DQL_GET_WEBCLOCK_FOR_NAME = "select holder as %s from dbo.webc_lock_s where object_name = '%s'";

    // not in use currently
    public static final String DQL_GET_RECORDS_ALL_VERSION = "select r_object_id from cchis_records (all) where folder('%s', descend)";
    // not in use currently
    public static final String DQL_GET_RECORDS = "select r_object_id, i_chronicle_id from cchis_records where folder('%s', descend)";
    // not in use currently
    public static final String DQL_GET_LAST_APPROVED = "select r_object_id from cchis_records (all) where i_chronicle_id = '%s' and any r_version_label = 'Approved'";
    public static final String DQL_QUAL_GET_LAST_APPROVED = "cchis_records (all) where i_chronicle_id = '%s' and any r_version_label = 'Approved'";


    public static final String DQL_GET_ALL_VERSIONS = "select distinct r_version_label from %s (all) where folder('%s', descend)";

    public static final String DQL_GET_NAMES = "select distinct r_object_id, object_name, r_folder_path, con.i_full_format from %s (all) c, dm_folder f, dmr_content con where f.r_object_id = c.i_folder_id and con.r_object_id = c.i_contents_id and folder('%s', descend) and r_folder_path != '' and r_folder_path like '/icspipeline.com%%' and any r_version_label = '%s' %s enable(row_based)";
    public static final String DQL_GET_DUPLICATE_NAMES_COUNT = "select count(*) as c from %s (all) where folder('%s', descend) and any r_version_label = '%s' and (lower(object_name) = lower('%s') or lower(object_name) = lower('%s'))";
    public static final String DQL_GET_DUPLICATE_NAMES_IDS = "select distinct r_object_id from %s (all) where folder('%s', descend) and any r_version_label = '%s' and (lower(object_name) = lower('%s') or lower(object_name) = lower('%s')) and r_object_id != '%s'";

    public static final String DQL_GET_NAMES_WRONG_FORMAT = "select distinct r_object_id, object_name, r_folder_path from %s (all) c, dm_folder f, dmr_content con where f.r_object_id = c.i_folder_id and con.r_object_id = c.i_contents_id and folder('%s', descend) and r_folder_path != '' and r_folder_path like '/icspipeline.com%%' and any r_version_label = '%s' and con.i_full_format != '%s' enable(row_based)";

    public static final String DQL_COUNT_DOCUMENTS = "select count(r_object_id) as c from %s (all) where folder('%s', descend)";
    public static final String DQL_FOLDER = " folder('%s', descend)";
    public static final String DQL_COUNT_DOCUMENTS_WITH_VERSION = "select count(r_object_id) as c from %s (all) where folder('%s', descend) and any r_version_label = '%s'";
    public static final String DQL_WITH_VERSION_POSTFIX = " and any r_version_label = '%s'";
    public static final String DQL_GET_DOCUMENTS = "select r_object_id from %s (all) where folder('%s', descend)";
    public static final String DQL_GET_DOCUMENTS_WITH_VERSION = "select r_object_id from %s (all) where folder('%s', descend) and any r_version_label = '%s'";

    public static final String DQL_MODIFIED_DATEFROM_POSTFIX = " and r_modify_date >= '%s'";
    public static final String DQL_MODIFIED_DATETO_POSTFIX = " and r_modify_date < '%s'";

    public static final String DQL_COUNT_MODIFIED_DOCUMENTS = "select count(r_object_id) as c from dm_document (all) where folder('/icspipeline.com', descend) and r_modify_date > '%s'";
    public static final String DQL_GET_MODIFIED_DOCUMENTS = "select r_object_id, i_vstamp, i_is_replica, r_aspect_name, i_is_reference from %s (all) where folder('/icspipeline.com', descend) and r_modify_date >= '%s'";

    public static final String DQL_GET_WEBC_BY_FOLDER_NAME = "select r_object_id from dm_webc_config c, dm_folder f where f.r_object_id = c.source_folder_id and any f.r_folder_path = '%s'";
    public static final String DQL_QUAL_GET_WEBC_BY_FOLDER_NAME = "dm_webc_config c, dm_folder f where f.r_object_id = c.source_folder_id and any f.r_folder_path = '%s'";


    public static final String SQL_JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    public static final String INVALID = "INVALID_";
    public static final String INVALID_JDBC_DRIVER = INVALID + SQL_JDBC_DRIVER;
    public static final String SQL_GET_WEBC_REG_TABLES = "select object_name from dm_registered_sp where object_name like 'dm_webc_8%'";
    public static final String SQL_GET_ALL_WEBC_TABLES = "select TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_NAME like 'dm_webc_8%'";
    public static final String SQL_GET_TEMP_OBJECTS = "SELECT TABLE_NAME, TABLE_TYPE FROM %s.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '%s%%' AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')";
    public static final String SQL_DROP_TEMP_OBJECTS = "IF OBJECT_ID('dbo.%1$s', '%3$s') IS NOT NULL DROP %2$s dbo.%1$s";
    public static final String SQL_DROP_UNREG_TABLES_S = "IF OBJECT_ID('DM_ICSPipelineProd_docbase.dbo.dm_webc_%1$s_s', 'U') IS NOT NULL DROP TABLE DM_ICSPipelineProd_docbase.dbo.dm_webc_%1$s_s";
    public static final String SQL_DROP_UNREG_TABLES_R = "IF OBJECT_ID('DM_ICSPipelineProd_docbase.dbo.dm_webc_%1$s_r', 'U') IS NOT NULL DROP TABLE DM_ICSPipelineProd_docbase.dbo.dm_webc_%1$s_r";
    public static final String SQL_DROP_UNREG_TABLES_M = "IF OBJECT_ID('DM_ICSPipelineProd_docbase.dbo.dm_webc_%1$s_m', 'U') IS NOT NULL DROP TABLE DM_ICSPipelineProd_docbase.dbo.dm_webc_%1$s_m";
    public static final String SQL_CREATE_INDEXES = "if not exists (select 1 from sys.indexes where object_id = OBJECT_ID('%2$s') and name = '%1$s') create nonclustered index %1$s on %2$s (%3$s) %4$s ";
    public static final String SQL_CREATE_INDEXES_INCLUDE = "include (%s)";

    public static final String SQL_DROP_INDEXES = "if exists (select 1 from sys.indexes where object_id = OBJECT_ID('%2$s') and name = '%1$s') drop index %1$s on %2$s";

    public static final String SQL_INDEX_DMR_CONTENT_I_PARKED_STATE = "dmr_content_i_parked_state";

    public static final String ATTR_JDBC_DRIVER = "jdbc_driver";
    public static final String ATTR_R_VERSION_LABEL = "dm_sysobject.r_version_label";
    public static final String ATTR_R_OBJECT_TYPE = "dm_sysobject.r_object_type";
    public static final String ATTR_R_CURRENT_STATE = "dm_sysobject.r_current_state";
    public static final String ATTR_EXTRA_ARGS = "extra_arguments";
    public static final String ATTR_A_CURRENT_STATUS = "a_current_status";
    public static final String SUBMITTED = "SUBMITTED";
    public static final String PIPELINE = "/icspipeline.com";

    public static final String STRING_TYPE = "2";
    public static final String INTEGER_TYPE = "1";
    public static final String BOOLEAN_TYPE = "0";
    public static final String ZERO = "0";
    public static final String THIRTY_TWO = "32";
    public static final String FALSE_SYM = "F";
    public static final String TRUE_SYM = "T";
    public static final String PIPE = "|";
    public static final String SLASH = "/";
    public static final String UNDERSCORE = "_";
    public static final String COMMA = ",";
    public static final String SEMICOLON = ";";
    public static final String COLON = ":";
    public static final String ARROW = " -> ";
    public static final String NEW_LINE = "\n";
    public static final String TRUE = "true";


    public static final String COUNT_ALIAS = "c";

    public static final String DCTM_R_OBJECT_ID = "r_object_id";
    public static final String DCTM_R_CURRENT_STATE = "r_current_state";
    public static final String DCTM_I_CHRONICLE_ID = "i_chronicle_id";
    public static final String DCTM_R_VERSION_LABEL = "r_version_label";
    public static final String DCTM_R_FOLDER_PATH = "r_folder_path";
    public static final String DCTM_OBJECT_NAME = "object_name";
    public static final String DCTM_CCHIS_RECORDS = "cchis_records";
    public static final String DCTM_DM_DOCUMENT = "dm_document";
    public static final String DCTM_I_FULL_FORMAT = "i_full_format";
    public static final String DCTM_WEBC_CONFIG = "dm_webc_config";
    public static final String DCTM_WEBC_TARGET = "dm_webc_target";
    public static final String DCTM_DM_JOB = "dm_job";

    public static final String STR_EMPTY_STRING = "";
    public static final String OR = " OR ";
    public static final String HTTP_RESPONSE_RESULT = "result";
    public static final String HTTP_RESPONSE_STATUS = "http_response_status";
    public static final String HTTP_RESPONSE_TIMED_OUT = "timed_out";
    public static final String HTTP_RESPONSE_OK = "HTTP/1.1 200 OK";
    public static final String HTTP_RESPONSE_SERVER_ERROR = "HTTP/1.1 500 Internal Server Error";

    public static final String MSG_DESTROY_REG_TABLE = "Registered tables destroying. Success: %d. Failed: %s";
    public static final String MSG_DESTROY_UNREG_TABLE = "Unregistered table deletion. Success: %d. Failed: %d";
    public static final String MSG_CLEAN_LOCK_TABLE = "Webc_lock table cleaning. Success: %d. Failed: %d";
    public static final String MSG_CREATE_INDEXES = "Indexes creation. Success: %d. Failed: %d";
    public static final String MSG_DROP_INDEXES = "Indexes dropping. Success: %d. Failed: %d";

    public static final String MSG_DESTROY_TEMP_TABLE = "temp objects deletion. Success: %d. Failed: %d";
    public static final String MSG_DESTROY_CONFIG = "Destroy config %s. Last status: %s";
    public static final String MSG_PUBLISH_CONFIG = "Publish config %s";
    public static final String MSG_CONFIG_PUBLISHED = "%s published";
    public static final String MSG_PUBLISH_FAILED = "Publishing process failed for %s";
    public static final String MSG_PUBLISH_PARENT_FAILED = "Publishing failed for parent config %s. See logWithDetails file for details.";

    public static final String MSG_PUBLISH_CONFIG_ALL_VERSIONS_SUCCESS = "Published successfully (all versions): %s";
    public static final String MSG_PUBLISH_CONFIG_ALL_VERSIONS_FAILURE = "Not published successfully (all versions): %s";
    public static final String MSG_CREATED_SUBCONFIG = "Config %s is created. Parent config %s";
    public static final String MSG_CONFIG_APPEND_ATTR = "Attrubute %s appended to config %s";

    public static final String MSG_COPY_CONFIG = "Create copy of %s";
    public static final String MSG_LOCK_OWNER = "Lock owner %s";
    public static final String MSG_WEBC_LOCK_COUNT = "Webclock count for %s: %d";
    public static final String MSG_WEBC_LOCK_COUNT_HOLDER = "Webclock count/holder for %s: %s/%s";
    public static final String MSG_JOB_STATUS = "Job status for %s: %s";
    public static final String MSG_WEBC_STATUS = "Config status for %s: %s";
    public static final String MSG_DESTROY_DOCUMENT = "Documents deletion. Success: %d. Failed: %d";
    public static final String MSG_CONFIG_ALL_DOCUMENTS_VERSIONS = "Config %s (%d) : %s";

    public static final String MSG_SCS_EXTRA_ARGS_SET = "SCS extra arguments are set";
    public static final String MSG_SCS_EXTRA_ARGS_UNSET = "SCS extra arguments are unset";
    public static final String MSG_SCS_JDBC_DRIVER_SET = "SCS JDBC Driver is set to fake";
    public static final String MSG_SCS_JDBC_DRIVER_UNSET = "SCS JDBC Driver is set to default";
    public static final String MSG_DM_WEBCACHE_PUBLISH_SET = "dm_webcache_publish timeout is set";
    public static final String MSG_DM_WEBCACHE_PUBLISH_UNSET = "dm_webcache_publish timeout is unset to default";
    public static final String MSG_SCS_FOLDER_NO_ACCESS = "Cannot access IDS folder : %s";

    public static final String MSG_WARN_PUBLISH_SUBCONFIG = "Error publishing %s.";
    public static final String MSG_ERROR_PUBLISH_SUBCONFIG = "Error publishing %s. Parent config is %s";
    public static final String MSG_ERROR_PUBLISH_TIMEOUT = "Exceeding timeout when publishing %s.";
    public static final String MSG_PUBLISH_SUBCONFIG = "%s is published. Parent config is %s";
    public static final String MSG_PUBLISH_WAIT_UNLOCK = "Waiting for %s to be unlocked";

    public static final String MSG_EXECUTOR_SHUTDOWN = "shutdown finished";
    public static final String MSG_EXECUTOR_CANCEL_TASKS = "cancel non-finished tasks";
    public static final String MSG_EXECUTOR_SHUTDOWN_ERROR = "task interrupted";

    public static final String MSG_WEBCLOCK_NOT_FOUND_ERROR = "Couldn't find webc_lock for %s";
    public static final String MSG_WEBCLOCK_NOT_FOUND_ERROR_PROGRESS = MSG_WEBCLOCK_NOT_FOUND_ERROR + ". Another try ...";
    public static final String MSG_EXPORT_SET_ERROR = "Cannot get export set for %s";
    public static final String MSG_EXPORT_SET_LENGTH = "Export set dirs length for %s is %d";
    public static final String MSG_EXPORT_SET_DIRS = "Export Set Dir(s) for %s : %s";
    public static final String MSG_ERROR_EXPORT_SET_DIRS_NOT_FOUND = "Not found Export Set Dir(s) for %s : %s";
    public static final String MSG_ZIP_CREATED = "Created successfully %s";
    public static final String MSG_CONFIG_REPORT_INVALID = "ConfigReport for %s is not valid. exportSetCount: %d. unknownProblemCount: %d";


    public static final String MSG_ERROR_PROPERTIES = "Cannot load properties";
    public static final String MSG_ERROR_IS_WEBC_LOCKED = "Error checking if config %s is locked: ";
    public static final String MSG_ERROR_GET_CONFIG = "Error getting config %s";
    public static final String MSG_ERROR_GET_CONFIG_INFO = "Error getting config information %s";
    public static final String MSG_ERROR_DELETE_DIR = "Error deleting directory %s: %s";
    public static final String MSG_ERROR_CLEAN_DIR = "Error cleaning directory %s: %s";
    public static final String MSG_DELETE_SCS_DIR_SUCCESS = "Successfully deleted Target Root Directory";
    public static final String MSG_ERROR_DELETE_SCS_DIR = "Error deleting Target Root Directory";
    public static final String MSG_DELETE_BATCH_DIR_SUCCESS = "Successfully deleted Batches Directory";
    public static final String MSG_ERROR_DELETE_BATCH_DIR = "Error deleting Batches Directory";
    public static final String MSG_DELETE_DUP_NAMES_DIR_SUCCESS = "Successfully deleted Duplicate Names Directory";
    public static final String MSG_ERROR_DELETE_DUP_NAMES_DIR = "Error deleting Duplicate Names Directory";
    public static final String MSG_DELETE_EXPORT_SET_SUCCESS = "Successfully deleted export sets for %s";
    public static final String MSG_ERROR_DELETE_EXPORT_SET = "Failed to delete export sets for %s: %s";
    public static final String MSG_ERROR_COUNT_THRESHOLD_EXCEEDED = "Server Error Count %s >= %s";
    public static final String MSG_RECORD_COUNT_REPORT = "Records count under %s. All/published (export set)/not published/wrong format/duplicate names/unknown: %d/%d/%d/%d/%d+%d/%d";
    public static final String MSG_NOT_PUBLISHED_RECORDS = "Not published records for %s: %s.";
    public static final String MSG_WRONG_FORMAT_RECORDS = "Wrong format records for %s: %s";
    public static final String MSG_DUPLICATE_NAMES_RECORDS = "Duplicate names records for %s: %s";
    public static final String MSG_DEFECT_DUPLICATE_NAMES_RECORDS = "Defect duplicate names records for %s: %s";

    public static final String MSG_UNKNOWN_PROBLEM_RECORDS = "Problematic records with unknown reason for %s: %s";
    public static final String MSG_ERROR_UNKNOWN_WEBC_TYPE = "Unknown webc type";
    public static final String MSG_ERROR_UNKNOWN_PUBLISH_TYPE = "Unknown publish type";
    public static final String MSG_ERROR_GET_RECORD_COUNT_REPORT = "Unable get the reason why counts do not match for %s.";

    public static final String MSG_WEBC_DQL = "Webc DQL: %s";
    public static final String MSG_REG_TABLE_DQL = "Reg table DQL: %s";
    public static final String MSG_ERROR_OPEN_CONNECTION = "Cannot open SQL Connection";
    public static final String MSG_ERROR_GET_CONNECTION = "Cannot get SQL Connection";
    public static final String MSG_SQL_SERVER_CONNECTED = "SQL Server: connected";
    public static final String MSG_SQL_SERVER_DISCONNECTED = "SQL Server: disconnected";
    public static final String MSG_WEBC_TABLES_COUNT = "Reg table count: %s, all table count: %s";
    public static final String MSG_SQL_BATCH_ERROR = "Error executing SQL batch";
    public static final String MSG_DROP_UNREG_TABLE_SUCCESS = "Unregistered tables were dropped. Success: %s. Failed: %s";

    public static final String MSG_SERVER_ERROR_COUNTER = "Server Error Counter Value: %d";

    public static final String IDS_SOURCE_PATH = "d:\\Documentum\\share\\temp\\web_publish";
    public static final String IDS_TARGET_PATH = "d:\\Documentum\\IDSTarget\\data\\2788";
    public static final String IDS_RESPONCE_DIR = "D:\\Documentum\\product\\6.7\\webcache\\temp";
    public static final File IDS_RESPONCE_DIR_FILE = new File(IDS_RESPONCE_DIR);

    public static final String R_VERSION_LABEL_PATTERN = "^\\d+\\.\\d+(\\.\\d+\\.\\d+)?";
    public static final String R_BRANCH_VERSION_LABEL_PATTERN = "^\\d+\\.\\d+\\.\\d+\\.\\d+";


    public static final String EXPORT_SET_FOLDER_PREFIX = "webcache_dir_";
    public static final String CONTENT_DIR = "content_dir";

    public static final String DEFAULT_INSIGHT_EXT = ".htm";
    public static final String DEFAULT_INSOURCE_EXT = ".xml";
    public static final String DEFAULT_IMAGE_EXT = ".gif";
    public static final String ZIP_EXT = ".zip";

    public static final String TEMP = "TEMP";
    public static final String DASH = "-";
    public static final String TEMP_PREFIX = TEMP + DASH;
    public static final String DELTA = "DELTA";
    public static final String DELTA_PREFIX = DELTA + DASH;
    public static final String EMPTY_STRING = StringUtils.EMPTY;
    public static final String SPACE = " ";
    public static final String DOT = ".";
    public static final String DOT_REGEX = "\\.";

    public static final String MSG_ERROR = "ERROR: ";

    public static final String READY_TO_IMPORT = "READY_TO_IMPORT";
    public static final String WRONG_FORMAT = "WRONG_FORMAT";
    public static final String DUPLICATE_NAME = "DUPLICATE_NAMES";
    public static final String BACKED_RESPONSES = "BACKED_RESPONSES";
    public static final String ERROR_RESPONSES = "ERROR_RESPONSES";
    public static final String INVALID_EXPORT_SETS = "INVALID_EXPORT_SETS";
    public static final String DUPLICATE_NAME_POSTFIX = DASH + DUPLICATE_NAME;

    public static final int DEFAULT_PUBLISH_METHOD = 1;
    public static final int SINGLE_ITEM_PUBLISH_METHOD = 5;
    public static final int PUBLISH_FREE_METHOD = 6;
    public static final boolean DEFAUL_SINGLE_OPTION = true;

    //public static final int WEBC_LOCK_TIMEOUT = 1800; // externalized
    //public static final String PUBLISH_TIMEOUT = "600"; // externalized
    //public static final int JOB_INTERVAL = 3; // externalized
    //public static final int WEBC_LOCK_INTERVAL = 10; // externalized
    public static final int SLOW_CONFIG_EXECUTOR_COUNT = 10;
    public static final int BOOK_EXECUTOR_COUNT = 3;
    public static final String PUBLISH_TIMEOUT_SLOW_CONFIG = "3000";
    public static final int WEBC_LOCK_TIMEOUT_SLOW_CONFIG = 3000;
    public static final int JOB_TIMEOUT_SLOW_CONFIG = 3000;


    public static final int DEFAULT_INTERVAL = 1;
    public static final int TIME_CREATE_SLOW_CONFIG_INTERVAL = 3;
    public static final int TIME_TO_LOCK = 3;
    public static final int TIME_TO_DESTROY_WEBC = 3;

    public static final String IDS_TARGET_SERVICE = "EMC IDS Target_non_secure_2788";
    public static final String IDS_SOURCE_SERVICE = "DmWEBCACHE";
    public static final String JMS_SERVICE = "DmMethodServer";

    public static final int IDS_RESTART_DEFAULT_THRESHOLD = 50;
    public static final int IDS_RESTART_ERROR_THRESHOLD = 50;
    public static final int TARGET_STOP_TIMEOUT = 120;
    public static final int TARGET_START_TIMEOUT = 120;
    public static final int TARGET_DELAY = 0;
    public static final int JBOSS_STOP_TIMEOUT = 120;
    public static final int JBOSS_START_TIMEOUT = 300;
    public static final int JBOSS_DELAY = 60;
    public static final int END_DELAY = 3;
    public static final int DEFAULT_SHUTDOWN_TIMEOUT = 5;
    public static final int CLEANUP_SHUTDOWN_TIMEOUT = 180;

    public static final int MAX_CONCURRENT_SESSIONS = 70;

    public static final int MAX_SINGLE_ITEMS = 400;

    public static final boolean GLOBAL_INIT;

    public static final String PUBLISH_TIMEOUT;
    public static final boolean CREATE_INDEXES;
    public static final boolean SET_SCS_EXTRA_ARGS;
    public static final String REPORT_FOLDER_PATH;
    public static final boolean DETAILED_REPORT;
    public static final int EXECUTOR_COUNT;
    public static final String EXPORT_DIR_PATH;
    public static final String TARGET_ROOT_DIR_PATH;
    public static final int STATUS_CHECK_INTERVAL;
    public static final int WEBC_LOCK_INTERVAL;
    public static final String PUBLISH_LOG_LEVEL;
    public static final int PUBLISH_LOG_LEVEL_INT;

    public static final String SCS_DB_NAME;
    public static final String USER;
    public static final String PASSWORD;

    public static final String BATCH_TEMP_DIR_PATH;
    public static final File BATCH_TEMP_DIR;
    public static final boolean RETRY_IF_ERROR;
    public static final boolean TARGET_SYNC_DISABLED;
    public static final String PRE_SYNC_SCRIPT;
    public static final String SCS_TARGET_PATH;
    public static final boolean USE_BOOK_CACHE;
    public static final boolean LAUNCH_BOOK_ASYNC;

    public static final boolean USE_PUBLISHING;


    static {
        PropertiesHolder properties = PropertiesHolder.getInstance();

        USE_PUBLISHING = Boolean.parseBoolean(properties.getProperty(PROP_USE_PUBLISHING));
        GLOBAL_INIT =  Boolean.parseBoolean(properties.getProperty(PROP_GLOBAL_INIT));
        PUBLISH_TIMEOUT = properties.getProperty(PROP_PUBLISH_TIMEOUT);
        CREATE_INDEXES = Boolean.parseBoolean(properties.getProperty(PROP_CREATE_INDEXES));
        SET_SCS_EXTRA_ARGS = Boolean.parseBoolean(properties.getProperty(PROP_SET_SCS_EXTRA_ARGS));
        REPORT_FOLDER_PATH = FilenameUtils.separatorsToSystem(properties.getProperty(PROP_REPORT_DIR));
        DETAILED_REPORT = Boolean.parseBoolean(properties.getProperty(PROP_DETAILED_REPORT));
        EXECUTOR_COUNT = Integer.parseInt(properties.getProperty(PROP_EXECUTOR_COUNT));
        EXPORT_DIR_PATH = FilenameUtils.separatorsToSystem(properties.getProperty(PROP_EXPORT_DIR));
        TARGET_ROOT_DIR_PATH = FilenameUtils.separatorsToSystem(properties.getProperty(PROP_TARGET_ROOT));
        STATUS_CHECK_INTERVAL = Integer.parseInt(properties.getProperty(PROP_STATUS_INTERVAL));
        WEBC_LOCK_INTERVAL = Integer.parseInt(properties.getProperty(PROP_LOCK_INTERVAL));
        PUBLISH_LOG_LEVEL = properties.getProperty(PROP_PUBLISH_LOG_LEVEL);
        PUBLISH_LOG_LEVEL_INT = Integer.parseInt(PUBLISH_LOG_LEVEL);

        USER = properties.getProperty(PROP_SUPERUSER_NAME);
        PASSWORD = properties.getProperty(PROP_SUPERUSER_PSWD);
        SCS_DB_NAME = properties.getProperty(PROP_DB_SCS_NAME);

        String targetHost1 = properties.getProperty(PROP_SCS_TARGET_HOST_1);
        String sourceHost1 = properties.getProperty(PROP_SCS_SOURCE_HOST_1);
        SCS_TARGET_PATH = sourceHost1.equals(targetHost1) ? IDS_TARGET_PATH
                : FilenameUtils.separatorsToSystem(properties.getProperty(PROP_SCS_TARGET_SHARE));

        BATCH_TEMP_DIR_PATH = properties.getProperty(PROP_BATCH_TEMP_DIR);
        BATCH_TEMP_DIR = new File(BATCH_TEMP_DIR_PATH);
        FileUtil.mkdirs(BATCH_TEMP_DIR.getParentFile());

        RETRY_IF_ERROR = Boolean.parseBoolean(properties.getProperty(PROP_RETRY_IF_ERROR));
        TARGET_SYNC_DISABLED = Boolean.parseBoolean(properties.getProperty(PROP_TARGET_SYNC_DISABLED));
        PRE_SYNC_SCRIPT = properties.getProperty(PROP_PRE_SYNC_SCRIPT);

        USE_BOOK_CACHE = Boolean.parseBoolean(properties.getProperty(PROP_USE_BOOK_CACHE));
        LAUNCH_BOOK_ASYNC = Boolean.parseBoolean(properties.getProperty(PROP_LAUNCH_BOOK_ASYNC));

    }

}

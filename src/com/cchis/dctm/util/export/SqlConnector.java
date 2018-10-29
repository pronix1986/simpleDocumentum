package com.cchis.dctm.util.export;

import com.cchis.dctm.util.export.exception.ExportException;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import static com.cchis.dctm.util.export.util.ExportConstants.*;

/**
 *
 */
public class SqlConnector {

    private static final Logger LOG = Logger.getLogger(SqlConnector.class);

    private static final SqlConnector INSTANCE = new SqlConnector();
    private static final String MSG_INFO_OPEN_CONNECTION = "Open connection";
    private static final String MSG_WARN_CANNOT_OPEN_CONNECTION = "Cannot open sql connection";
    private Connection connection = null;
    private String dbUser = EMPTY_STRING;
    private String dbPassword = EMPTY_STRING;
    private String dbUrl = EMPTY_STRING;

    public static SqlConnector getInstance() {
        return INSTANCE;
    }

    private SqlConnector() {
        try {
            PropertiesHolder properties = PropertiesHolder.getInstance();
            dbUser = properties.getProperty(PROP_DB_USER_NAME);
            dbPassword = properties.getProperty(PROP_DB_USER_PSWD);
            dbUrl = properties.getProperty(PROP_DB_URL);

            getSqlConnection();
        } catch(Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR_OPEN_CONNECTION, e);
        }
    }

    public Connection getSqlConnection() {
        try {
            if (connection == null || connection.isClosed()) {
                connection = openSqlConnection();
            }
            if (!connection.isValid(0)) {
                closeSqlConnection();
                connection = openSqlConnection();
            }
            return connection;
        } catch(Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR_GET_CONNECTION, e);
            return null;
        }
    }

    public void closeSqlConnection() {
        try {
            if (!connection.isClosed()) {
                LOG.info(MSG_SQL_SERVER_DISCONNECTED);
                connection.close();
            }
        } catch (SQLException e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
        }
    }

    private Connection openSqlConnection() {
        try {
            Class.forName(SQL_JDBC_DRIVER);
            Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
            conn.setAutoCommit(true);
            LOG.debug(MSG_INFO_OPEN_CONNECTION);
            return conn;
        } catch (Exception e) {
            LOG.warn(MSG_WARN_CANNOT_OPEN_CONNECTION);
            throw new ExportException(e);
        }
    }

    public static void closeSilently(Statement statement) {
        try {
            if (statement != null) statement.close();
        } catch (SQLException e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
        }
    }

    public static void executeBatch(Statement statement, String logMessage) {
        final Counter counter = new Counter(false);
        try {
            Arrays.stream(statement.executeBatch()).forEach(result -> {
                if (result == Statement.EXECUTE_FAILED) {
                    LOG.debug(String.format("Error executing #%d in batch", result));
                    counter.incrementFailed();
                } else {
                    counter.incrementSuccess();
                }
            });
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_SQL_BATCH_ERROR, e);
        } finally {
            if (counter.getCount() > 0) {
                LOG.debug(String.format(logMessage, counter.getSuccess(), counter.getFailed()));
            }
        }
    }
}

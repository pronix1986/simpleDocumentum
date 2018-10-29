package com.cchis.dctm.util.export;

import com.documentum.com.DfClientX;
import com.documentum.com.IDfClientX;
import com.documentum.fc.client.*;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.IDfLoginInfo;
import com.documentum.wcm.IWcmAppContext;
import com.documentum.wcm.WcmAppContext;
import org.apache.commons.collections.IteratorUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Locale;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.cchis.dctm.util.export.util.ExportConstants.*;


public class SessionManagerHandler {

    private static final Logger LOG = Logger.getLogger(SessionManagerHandler.class);
    private static final Locale DEFAULT_LOCALE = Locale.US;
    private static final String DEFAULT_LOCALE_STR = DEFAULT_LOCALE.toString();

    private static final SessionManagerHandler INSTANCE = new SessionManagerHandler();


    private static final Lock NEW_SESSION_LOCK = new ReentrantLock();
    private static final Condition GET_NEW_SESSION_CONDITION = NEW_SESSION_LOCK.newCondition();

    public static SessionManagerHandler getInstance() {
        return INSTANCE;
    }

    private final String user;
    private final String pswd;
    private final String docbase;

    private IDfClientX clientX;
    private IDfSessionManager sessionManager;
    private IWcmAppContext wcmAppContext;

    private IDfSession mainSession;
    private IDfSession cleanupSession;


    private SessionManagerHandler() {

        PropertiesHolder properties = PropertiesHolder.getInstance();
        user = properties.getProperty(PROP_SUPERUSER_NAME);
        pswd = properties.getProperty(PROP_SUPERUSER_PSWD);
        docbase = properties.getProperty(PROP_DOCBASE_NAME);

        try {
            clientX = new DfClientX();
            IDfClient client = clientX.getLocalClient();
            sessionManager = client.newSessionManager();
            sessionManager.getConfig().setLocale(DEFAULT_LOCALE_STR);
            IDfLoginInfo loginInfo = clientX.getLoginInfo();
            loginInfo.setUser(user);
            loginInfo.setPassword(pswd);
            loginInfo.setDomain(STR_EMPTY_STRING);
            sessionManager.setIdentity(docbase, loginInfo);
            mainSession = getSession();
            wcmAppContext = new WcmAppContext(sessionManager, docbase, DEFAULT_LOCALE, new Hashtable(), null, null);
        } catch (Exception e) {
            LOG.error(MSG_ERROR, e);
            System.exit(1);
        }
    }

    public IDfClientX getClientX() {
        return clientX;
    }

    public IDfSessionManager getSessionManager() {
        return sessionManager;
    }

    public IWcmAppContext getWcmAppContext() {
        return wcmAppContext;
    }

    public IDfSession getSession() {
        IDfSession session = null;
        try {
            session = sessionManager.getSession(docbase);
        } catch (Exception e) {
            LOG.error(MSG_ERROR, e);
        }
        return session;
    }

    public IDfSession getMainSession() {
        if (mainSession == null || !mainSession.isConnected()) {
            releaseSession(mainSession);
            mainSession = getSession();
        }
        return mainSession;
    }

    public IDfSession getCleanupSession() {
        if (cleanupSession == null || !cleanupSession.isConnected()) {
            releaseSession(cleanupSession);
            cleanupSession = newSession();
        }
        return cleanupSession;
    }

    public IDfSession newSession() {
        IDfSession session = null;
        try {
            if (!waitNewSessionAvailable()) {
                session = sessionManager.newSession(docbase);
            } else {
                session = newSessionWithLock();
            }
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR, e);
        }
        return session;
    }

    private IDfSession newSessionWithLock() throws DfException {
        IDfSession session;
        NEW_SESSION_LOCK.lock();
        try {
            while(waitNewSessionAvailable()) {
                LOG.debug("Waiting new session available");
                GET_NEW_SESSION_CONDITION.awaitUninterruptibly();
            }
            session = sessionManager.newSession(docbase);
        } finally {
            NEW_SESSION_LOCK.unlock();
        }
        return session;
    }

    private boolean waitNewSessionAvailable() {
        return getSessionsCount() > MAX_CONCURRENT_SESSIONS;
    }

    public int getSessionsCount() {
        if (sessionManager != null) {
            Iterator sessions = sessionManager.getStatistics().getSessions(docbase);
            return IteratorUtils.toList(sessions).size(); // StreamSupport.stream(Spliterators.spliteratorUnknownSize(sessions, Spliterator.IMMUTABLE), false).count()
        }
        return 0;
    }

    public void releaseSession(IDfSession session) {
        NEW_SESSION_LOCK.lock();
        try {
            if (sessionManager != null && session != null) {
                sessionManager.release(session);
                GET_NEW_SESSION_CONDITION.signalAll();
            }
        } finally {
            NEW_SESSION_LOCK.unlock();
        }
    }

    private void releaseSessions(IDfSession... sessions) {
        for (IDfSession session : sessions) {
            releaseSession(session);
        }
    }

    public void releaseMajorSessions() {
        releaseSessions(getMainSession(), getCleanupSession());
    }

    public void flushMainSession() {
        try {
            getMainSession().flushCache(true);
        } catch (DfException e) {
            LogHandler.logWithDetails(LOG, Level.WARN, MSG_ERROR, e);
        }
    }
}

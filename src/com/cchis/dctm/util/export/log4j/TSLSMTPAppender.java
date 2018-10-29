package com.cchis.dctm.util.export.log4j;

import org.apache.log4j.net.SMTPAppender;

import javax.mail.Authenticator;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import java.util.Properties;

public class TSLSMTPAppender extends SMTPAppender {
    private boolean startTLS = false;

    @Override
    protected Session createSession() {
        Session session = super.createSession();
        if (isStartTLS()) {
            Properties props = session.getProperties();
            //props.put("mail.smtp.socketFactory.port", String.valueOf(getSMTPPort()));
            props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
            props.put("mail.smtp.starttls.enable", "true");
        }
        return session;
    }

    public boolean isStartTLS() {
        return startTLS;
    }

    public void setStartTLS(boolean startTLS) {
        this.startTLS = startTLS;
    }
}

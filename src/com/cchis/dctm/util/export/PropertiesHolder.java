package com.cchis.dctm.util.export;

import com.cchis.dctm.util.export.exception.ExportException;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

import static com.cchis.dctm.util.export.util.ExportConstants.*;

public final class PropertiesHolder {

    private static final Logger LOG = Logger.getLogger(PropertiesHolder.class);

    private static final PropertiesHolder INSTANCE = new PropertiesHolder();
    private Properties merged;

    public static PropertiesHolder getInstance() {
        return INSTANCE;
    }

    private PropertiesHolder() {
        merged = mergeProperties(
                loadProperties(PROP_EXPORT_PROPERTIES),
                loadProperties(PROP_ENV_PROPERTIES)
        );
    }

    private Properties loadProperties(String path) {
        try (InputStream resourceAsStream = ExportUtil.class
                .getResourceAsStream(path)) {
            Properties property = new Properties();
            property.load(resourceAsStream);
            return property;
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, MSG_ERROR_PROPERTIES, e);
            throw new ExportException(MSG_ERROR_PROPERTIES);
        }
    }

    private Properties mergeProperties(Properties... properties) {
        if (merged == null) {
            merged = new Properties();
        }
        Arrays.stream(properties).forEach(merged::putAll);
        return merged;
    }

    public String getProperty(String key) {
        return merged.getProperty(key);
}

    public void setProperty(String key, String value) {
        merged.setProperty(key, value);
    }
}

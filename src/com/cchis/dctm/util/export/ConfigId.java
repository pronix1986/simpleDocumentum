package com.cchis.dctm.util.export;

import com.cchis.dctm.util.export.util.Util;
import com.documentum.admin.object.IDfWebCacheConfig;
import com.documentum.fc.common.DfException;
import org.apache.log4j.Logger;

import static com.cchis.dctm.util.export.util.ExportConstants.*;

public class ConfigId {
    private static final Logger LOG = Logger.getLogger(ConfigId.class);
    private String configId;
    private String configName;
    private String version;

    private ConfigId() {
        this.configId = EMPTY_STRING;
        this.configName = EMPTY_STRING;
        this.version = EMPTY_STRING;
    }

    private ConfigId(IDfWebCacheConfig config) throws DfException {
        this.configId = config.getObjectId().getId();
        this.configName = config.getObjectName();
        this.version = config.getConfigVersionLabels().getString(0);
    }

    public String getConfigId() {
        return configId;
    }

    public String getConfigName() {
        return configName;
    }

    public String getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return Util.joinStrings(SEMICOLON, configId, configName, version);
    }

    public String toCSVString() {
        return Util.joinStrings(COMMA, configId, configName, version);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConfigId configId1 = (ConfigId) o;

        if (configId != null ? !configId.equals(configId1.configId) : configId1.configId != null) return false;
        return configName != null ? configName.equals(configId1.configName) : configId1.configName == null;
    }

    @Override
    public int hashCode() {
        int result = configId != null ? configId.hashCode() : 0;
        result = 31 * result + (configName != null ? configName.hashCode() : 0);
        return result;
    }

    public static ConfigId getConfigId(IDfWebCacheConfig config) throws DfException {
        return new ConfigId(config);
    }

    public static ConfigId getConfigIdFailSafe(IDfWebCacheConfig config) {
        ConfigId cId = new ConfigId();
        try {
            cId = ConfigId.getConfigId(config);
        } catch(Exception e) {
            LOG.warn(String.format(MSG_ERROR_GET_CONFIG_INFO, cId));
        }
        return cId;
    }
}

package com.cchis.dctm.util.export;

import com.cchis.dctm.util.export.report.ReportRecord;
import com.cchis.dctm.util.export.util.FileUtil;
import com.documentum.admin.object.IDfWebCacheConfig;
import com.documentum.fc.client.IDfPersistentObject;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.client.IDfTypedObject;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.cchis.dctm.util.export.util.ExportConstants.UTF_8;

public class PropertiesXMLGenerator {
    private static final Logger LOG = Logger.getLogger(PropertiesXMLGenerator.class);

    private final IDfWebCacheConfig config;
    private final ConfigId cId;
    private final List<ReportRecord> records;
    private final File file;
    private int packageId = 0;

    public PropertiesXMLGenerator(IDfWebCacheConfig config, List<ReportRecord> records, File file) {
        this.config = config;
        this.cId = ConfigId.getConfigIdFailSafe(config);
        this.records = new ArrayList<>(records);
        this.file = file;
        FileUtil.mkdirs(file.getParentFile());
    }

    public String getAllAttributes(IDfTypedObject object) throws DfException {
        return object.getString("_xml_string");
    }

    public String generateAttrLine(String attrName, Object attrValue) {
        return new com.documentum.webcache.sync.p(attrName, attrValue).toString();
    }

    public String startXML() {
        return startXML(file.getName(), DateFormatUtils.ISO_DATETIME_FORMAT.format(new Date()));
    }

    public String startXML(String fileName, String timestamp) {
        StringBuilder builder = new StringBuilder("<?xml version=\"1.0\"?>\n");
        builder.append("<ice-payload\n");
        builder.append("    payload-id=\"" + fileName + "\"\n");
        builder.append("    timestamp=\"" + timestamp + "\"\n");
        builder.append("    ice.version=\"1.0\" >\n  <ice-header>\n    <ice-sender\n       sender-id=\"http://www.documentum.com/\"\n       name=\"Documentum\"\n       role=\"syndicator\"\n    />\n  </ice-header>\n  <ice-request\n     request-id=\"DCTM Webcache Properties Data\">\n");
        return builder.toString();
    }

    public String startPackage(boolean isConfig) {
        StringBuilder builder = new StringBuilder();
        builder.append("     <ice-package\n        atomic-use=\"true\"\n        confirmation=\"false\"\n        editable=\"false\"\n        show-credit=\"false\"\n");
        builder.append("      fullupdate=\"");
        builder.append("true");
        builder.append("\"\n");
        builder.append("      new-state=\"");
        builder.append(isConfig?"0_0":"0_publish_end");
        builder.append("\"\n");
        builder.append("      old-state=\"");
        builder.append(isConfig?"ICE-ANY":"0_0");
        builder.append("\"\n");
        builder.append("      package-id=\"");
        builder.append(packageId++);
        builder.append("\"\n");
        builder.append("    subscription-id=\"" + cId.getConfigId() + "\"\n");
        builder.append("      >\n");
        return builder.toString();
    }

    public String startItemPackage() {
        return startPackage(false);
    }

    public String startConfigPackage() {
        return startPackage(true);
    }

    public String getLanguageCode(IDfTypedObject object) throws DfException {
        return object.getString("language_code");
    }

    public String getItem(IDfTypedObject object, boolean isConfig, String relativePath, String name) throws DfException {
        StringBuilder builder = new StringBuilder();
        builder.append("        <ice-item\n");
        builder.append("          item-id=\"");
        builder.append(object.getId("i_chronicle_id").toString());
        builder.append("\"\n");
        if (!isConfig) {
            builder.append("          subscription-element=\"");
            builder.append(StringEscapeUtils.escapeXml(relativePath));
            builder.append("\"\n");
        }
        builder.append("          name=\"");
        builder.append(name);
        builder.append("\"\n");
        if (!isConfig) {
            builder.append("          content-filename=\"");
            builder.append(StringEscapeUtils.escapeXml(relativePath));
            builder.append("\"\n");
        }
        builder.append("          content-type=\"application/xml\"\n");
        if (!isConfig) {
            builder.append("          lang=\"");
            builder.append(getLanguageCode(object));
            builder.append("\"");
        }
        builder.append("        >\n");
        builder.append("          <dctm-object>\n");

        builder.append(getAllAttributes(object));

        if (!isConfig) {
            String var11 = object.getString("i_contents_id");
            builder.append(generateAttrLine("content_id", var11));
            String folderPath = DocumentHandler.getFolderName((IDfSysObject)object, config);
            builder.append(generateAttrLine("r_folder_path", folderPath));
            builder.append(generateAttrLine("folder_language_code", object.getString("language_code")));
            String var22 = var11;
            IDfPersistentObject var24 = config.getSession().getObject(DfUtil.toId(var22));
            if (null != var24) {
                builder.append(generateAttrLine("i_full_format", var24.getString("i_full_format")));
            }

            builder.append("             <dctm-content-ref url=\"");
            builder.append("file://$STAGE/");
            builder.append(StringEscapeUtils.escapeXml(relativePath));
            builder.append("\" />\n");
        }

        builder.append("          </dctm-object>\n");
        builder.append("        </ice-item>\n");
        return builder.toString();
    }

    public String getItem(IDfTypedObject object, String relativePath) throws DfException {
        return getItem(object, false, relativePath, "web cache item");
    }

    public String getConfig() throws DfException {
        return getItem(config, true, null, "web cache config");
    }

    public String getTarget() throws DfException {
        return getItem(config.getWebCacheTarget(0), true, null, "web cache target");
    }

    public String endItemPackage() {
        return "      </ice-package>\n";
    }

    public String endXML()  {
        return "    </ice-request>\n</ice-payload>\n";
    }

    /**
     * without config information
     * * @param config
     *
     * @return
     */
    public String generateXML() throws DfException {
        StringBuilder builder = new StringBuilder(startXML());
        builder.append(startConfigPackage());
        builder.append(getConfig());
        builder.append(getTarget());
        builder.append(endItemPackage());
        builder.append(startItemPackage());
        for (ReportRecord record : records) {
            builder.append(getItem(record.getObject(), record.getRelativePath(config)));
        }
        builder.append(endItemPackage());
        builder.append(endXML());
        return builder.toString();
    }

    public void writeXML() {
        try {
            String xml = generateXML();
            FileUtils.writeStringToFile(file, xml, UTF_8);
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, "error writing xml: " + file.getPath(), e);
        }
    }
}

package com.cchis.dctm.util.export.util.exec;

import com.cchis.dctm.util.export.LogHandler;
import com.cchis.dctm.util.export.exception.ExportException;
import com.cchis.dctm.util.export.util.FileUtil;
import com.cchis.dctm.util.export.util.PropertiesXMLHandler;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Predicate;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static com.cchis.dctm.util.export.util.ExportConstants.*;

public class LongFieldsValidator {

    private static final Logger LOG = Logger.getLogger(LongFieldsValidator.class);

    private static final String REGS = "REGS";
    private static final String BILL = "BILL";
    private static final String IMAGES = "IMAGES";
    private static final String INSOURCE = "INSOURCE";
    public static final String ICS_INSIGHT_BILL = "ics_insight_bill";
    public static final String ICS_INSIGHT_REG = "ics_insight_reg";
    public static final String ICS_INSOURCE = "ics_insource";


    private static SAXParser parser;

    public static void main(String[] args) {
        LOG.info("Start validation");
        try {

            SAXParserFactory factory = SAXParserFactory.newInstance();
            parser = factory.newSAXParser();
            new LongFieldsValidator().validate();
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, "", e);
        }
        LOG.info("End validation");

    }

    private void validate() throws IOException {

        File root = new File(EXPORT_DIR_PATH);

        Predicate<Path> zipFilter = path -> {
            String fileName = path.getFileName().toString();
            return fileName.endsWith(ZIP_EXT) && (fileName.contains(BILL) || fileName.contains(REGS));
        };

        FileUtil.findPaths(root, zipFilter).forEach(this::parseZip);
    }

    private void parseZip(Path path) {
        LOG.info("Start processing: " + path);
        try(ZipInputStream zis = new ZipInputStream(new FileInputStream(path.toFile()))) {
            ZipEntry entry = zis.getNextEntry();
            while (entry != null) {
                if(PROPERTIES_XML.endsWith(entry.getName())) {
                    parser.parse(zis, new PropertiesXMLHandler(getType(path)));
                    return;
                }
                entry = zis.getNextEntry();
            }
            zis.closeEntry();

        } catch(IOException | SAXException ex) {
            LogHandler.logWithDetails(LOG, "", ex);
        }
    }

    private String getType(Path path) {
        String fileName = path.getFileName().toString();
        if(fileName.contains(BILL)) {
            return ICS_INSIGHT_BILL;
        } else if(fileName.contains(REGS)) {
            return ICS_INSIGHT_REG;
        } else if(fileName.contains(IMAGES)) {
            return DCTM_DM_DOCUMENT;
        } else if(fileName.contains(INSOURCE)) {
            return ICS_INSOURCE;
        }  else throw new ExportException("Unknown type");
    }


}

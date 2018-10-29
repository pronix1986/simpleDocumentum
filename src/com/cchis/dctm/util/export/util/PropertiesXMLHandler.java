package com.cchis.dctm.util.export.util;

import com.cchis.dctm.util.export.LogHandler;
import com.cchis.dctm.util.export.SessionManagerHandler;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.common.DfException;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.cchis.dctm.util.export.util.ExportConstants.DCTM_R_OBJECT_ID;
import static com.cchis.dctm.util.export.util.ExportConstants.EMPTY_STRING;
import static com.cchis.dctm.util.export.util.LongFields.*;
import static com.cchis.dctm.util.export.util.LongFieldsValidator.ICS_INSIGHT_BILL;
import static com.cchis.dctm.util.export.util.LongFieldsValidator.ICS_INSIGHT_REG;

public class PropertiesXMLHandler extends DefaultHandler {

    private static final Logger LOG = Logger.getLogger(PropertiesXMLHandler.class);
    public static final String DCTM_ATTR = "dctm-attr";
    public static final String NAME = "name";
    public static final String ICE_ITEM = "ice-item";

    private final Map<String, LongFields> descriptions = new LinkedHashMap<>();
    private LongFields descs;
    private boolean processItem = false;
    private boolean processId = false;
    private boolean processDesc = false;
    private boolean processDesc2 = false;
    private boolean processReq = false;
    private boolean processEmailAlert = false;
    private String type;

    public PropertiesXMLHandler(String type) {
        this.type = type;
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        if(ICE_ITEM.equalsIgnoreCase(qName) && "web cache item".equalsIgnoreCase(attributes.getValue(NAME))
                && !attributes.getValue("subscription-element").matches("^\\d{4}")) {
            processItem = true;
            descs = new LongFields();
            descs.setType(type);
        }

        if(processItem && DCTM_ATTR.equalsIgnoreCase(qName) && DCTM_R_OBJECT_ID.equalsIgnoreCase(attributes.getValue(NAME))) {
            processId = true;
        }

        if(processItem && DCTM_ATTR.equalsIgnoreCase(qName) && DCTM_ICS_DESCRIPTION.equalsIgnoreCase(attributes.getValue(NAME))) {
            processDesc = true;
        }

        if(processItem && DCTM_ATTR.equalsIgnoreCase(qName) && DCTM_ICS_DESCRIPTION2.equalsIgnoreCase(attributes.getValue(NAME))) {
            processDesc2 = true;
        }

        if(processItem && DCTM_ATTR.equalsIgnoreCase(qName) && DCTM_ICS_REQUIREMENTS.equalsIgnoreCase(attributes.getValue(NAME))) {
            processReq = true;
        }

        if(processItem && DCTM_ATTR.equalsIgnoreCase(qName) && DCTM_ICS_EMAIL_ALERT.equalsIgnoreCase(attributes.getValue(NAME))) {
            processEmailAlert = true;
        }

    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if(ICE_ITEM.equalsIgnoreCase(qName) ) {
            if (descs != null && StringUtils.isNotEmpty(descs.getId())) {
                descriptions.put(descs.getId(), new LongFields(descs));
            }
            processItem = false;
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        if(processId) {
            descs.setId(new String(ch, start, length));
            processId = false;
        }

        if(processDesc) {
            descs.setDescription(normalizeString(new String(ch, start, length)));
            processDesc = false;
        }

        if(processDesc2) {
            descs.setDescription2(normalizeString(new String(ch, start, length)));
            processDesc2 = false;
        }

        if(processReq) {
            descs.setRequirements(normalizeString(new String(ch, start, length)));
            processReq = false;
        }

        if(processEmailAlert) {
            descs.setEmailAlert(normalizeString(new String(ch, start, length)));
            processEmailAlert = false;
        }

    }

    @Override
    public void endDocument() throws SAXException {
        IDfSession session = SessionManagerHandler.getInstance().getMainSession();
        String qual = " from %s (all) where r_object_id = '%s'";
        String dqlBuf = "select ";
        if (ICS_INSIGHT_BILL.equals(type)) {
            dqlBuf += Util.commaJoinStrings(DCTM_ICS_DESCRIPTION, DCTM_ICS_DESCRIPTION2, DCTM_ICS_REQUIREMENTS, DCTM_ICS_EMAIL_ALERT);
        } else if (ICS_INSIGHT_REG.equals(type)) {
            dqlBuf += Util.commaJoinStrings(DCTM_ICS_DESCRIPTION, DCTM_ICS_DESCRIPTION2, DCTM_ICS_REQUIREMENTS, DCTM_ICS_EMAIL_ALERT);
        }

        final String dql = dqlBuf + qual;

        LOG.debug(descriptions);


        descriptions.entrySet().forEach(entry -> {
            String id = entry.getKey();
            String desc = entry.getValue().getDescription();
            String desc2 = entry.getValue().getDescription2();
            String req = entry.getValue().getRequirements();
            String emailAlert = entry.getValue().getEmailAlert();
            try {
                if(Util.getCountByQualification(session, String.format(qual, type, id)) != 1) {
                    LOG.warn(id + " : Cannot compare. Can't find in repo ");
/*                    LOG.debug("\t" + desc);
                    LOG.debug("\t" + desc2);
                    LOG.debug("\t" + req);
                    LOG.debug("\t" + emailAlert);*/
                    return;
                }
            } catch (DfException e) {
                LogHandler.logWithDetails(LOG, "getCount", e);
                return;
            }


            String dctmDesc = null;
            String dctmDesc2 = null;
            String dctmReq = null;
            String dctmEmailAlert = null;
            String dctmFeedComments = null;

            IDfCollection collection;
            try {
                collection = Util.runQuery(session, String.format(dql, type, id));
                collection.next();
                dctmDesc = normalizeString(collection.getString(DCTM_ICS_DESCRIPTION));
                dctmDesc2 = normalizeString(collection.getString(DCTM_ICS_DESCRIPTION2));
                dctmReq = normalizeString(collection.getString(DCTM_ICS_REQUIREMENTS));
                dctmEmailAlert = normalizeString(collection.getString(DCTM_ICS_EMAIL_ALERT));
            } catch (DfException dfe) {
                LogHandler.logWithDetails(LOG, "getRecord", dfe);
            }

            if(desc.equals(dctmDesc) && desc2.equals(dctmDesc2)
                    && req.equals(dctmReq) && emailAlert.equals(dctmEmailAlert)) {
                //LOG.debug(id + " : " + "match");
            } else{
                LOG.warn(id + " : NOT match. ");
                LOG.debug("\t" + desc + " : " + dctmDesc + " : " + desc.equals(dctmDesc));
                LOG.debug("\t" + desc2 + " : " + dctmDesc2 + " : " + desc2.equals(dctmDesc2));
                LOG.debug("\t" + req + " : " + dctmReq + " : " + req.equals(dctmReq));
                LOG.debug("\t" + emailAlert + " : " + dctmEmailAlert + " : " + emailAlert.equals(dctmEmailAlert));
            }

         //   LOG.debug(id + " : [" + entry.getValue()[0] + ", " + entry.getValue()[1] + "]");
        });
    }

    private String normalizeString(String input) {
        if (input.equals("\n")) return EMPTY_STRING;
        input = StringUtils.chomp(input);   // have different issues with the last character. This might not cover all cases.
        return input;
    }
}

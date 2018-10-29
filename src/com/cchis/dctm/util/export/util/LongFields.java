package com.cchis.dctm.util.export.util;

public class LongFields {

    public static final String DCTM_ICS_DESCRIPTION = "ics_description";
    public static final String DCTM_ICS_DESCRIPTION2 = "ics_description2";
    public static final String DCTM_ICS_REQUIREMENTS = "ics_requirements";
    public static final String DCTM_ICS_EMAIL_ALERT = "ics_email_alert";
    //public static final String DCTM_ICS_FEED_COMMENTS = "ics_feed_comments"; // NOT published attribute
    //public static final String DCTM_ICS_EXPEDITE = "ics_expedite"; // Always empty
    public static final String DCTM_ICS_CAUTIONLINE = "ics_cautionline"; // TODO: ics_insource_chapter


    private String type;
    private String id;
    private String description;
    private String description2;
    private String requirements;
    private String emailAlert;

    public LongFields() {
    }

    public LongFields(LongFields copy) {
        this.type = copy.type;
        this.id = copy.id;
        this.description = copy.description;
        this.description2 = copy.description2;
        this.requirements = copy.requirements;
        this.emailAlert = copy.emailAlert;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getDescription2() {
        return description2;
    }

    public void setDescription2(String description2) {
        this.description2 = description2;
    }

    public String getRequirements() {
        return requirements;
    }

    public void setRequirements(String requirements) {
        this.requirements = requirements;
    }

    public String getEmailAlert() {
        return emailAlert;
    }

    public void setEmailAlert(String emailAlert) {
        this.emailAlert = emailAlert;
    }

    @Override
    public String toString() {
        return "{" +
                "type='" + type + '\'' +
                ", id='" + id + '\'' +
                ", description='" + description + '\'' +
                ", description2='" + description2 + '\'' +
                ", requirements='" + requirements + '\'' +
                ", emailAlert='" + emailAlert + '\'' +
                '}';
    }
}

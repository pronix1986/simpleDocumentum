package com.cchis.dctm.util.export.cli;

import com.cchis.dctm.util.export.PublishType;
import com.cchis.dctm.util.export.util.Util;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections.SetUtils;

import javax.print.DocFlavor;
import java.util.*;

import static com.cchis.dctm.util.export.util.ExportConstants.EMPTY_STRING;


public final class CLIOption {

    private static CLIOption current;

    private final PublishType publishType;
    private final String dql;
    private final Map<String, String> combDates = new HashMap<>();
    private static final Set<String> DUMMY_SET = Collections.singleton(EMPTY_STRING);

    public CLIOption(PublishType publishType, String dql, Map<String, String> combDates) {
        this.publishType = publishType;
        this.dql = dql;
        if(Util.isNotEmptyMap(combDates)) {
            this.combDates.putAll(combDates);
        }
        setCurrent(this);
    }

    public PublishType getPublishType() {
        return publishType;
    }

    public String getDql() {
        return dql;
    }

    public String getDates(String configName) {
        if (isSingleDate()) {
            return combDates.get(EMPTY_STRING);
        }
        return combDates.get(configName);
    }

    public String getDates() {
        return combDates.toString();
    }

    public static CLIOption getCurrent() {
        return current;
    }

    public static void setCurrent(CLIOption current) {
        CLIOption.current = current;
    }

    private boolean isSingleDate() {
        return combDates.size() == 1 && SetUtils.isEqualSet(combDates.keySet(), DUMMY_SET);
    }
}

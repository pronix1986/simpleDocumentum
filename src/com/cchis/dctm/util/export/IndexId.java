package com.cchis.dctm.util.export;

import com.cchis.dctm.util.export.util.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.cchis.dctm.util.export.util.ExportConstants.UNDERSCORE;

public class IndexId {

    private final String tableName;
    private List<String> columns = new ArrayList<>();
    private List<String> include = new ArrayList<>();

    public IndexId(String tableName, List<String> columns, List<String> include) {
        this.tableName = tableName;
        this.columns = columns;
        this.include = include;
    }

    public IndexId(String tableName, List<String> columns) {
        this(tableName, columns, null);
    }

    public IndexId(String tableName, String... columns) {
        this(tableName, Arrays.asList(columns));
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<String> getInclude() {
        return include;
    }

    @Override
    public String toString() {
        return tableName + UNDERSCORE
                + Util.simpleArrayToString(columns.toString(), UNDERSCORE);
    }
}

package com.cchis.dctm.util.export.report;

import com.cchis.dctm.util.export.util.FileUtil;
import com.cchis.dctm.util.export.util.Util;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.cchis.dctm.util.export.util.ExportConstants.*;

/**
 * For internal use only
 */
public class BookStartDateChecker extends AbstractReport {
    private static final Logger LOG = Logger.getLogger(BookStartDateChecker.class);

     @Override
    public List<String> getReportLines() {
        File root = new File(EXPORT_DIR_PATH);
         // MultiMap multiMap = new MultiHashMap(); // not generic in this version of commons library
         Map<String, List<BasicFileAttributes>> map = new LinkedHashMap<>();

        List<String> lines = new ArrayList<>();
        FileUtil.findPathsByPartName(root, ZIP_EXT).forEach(path ->
             map.merge(getConfigNameByPath(path), new ArrayList<>(Collections.singletonList(getFileAttributes(path))),
                 (oldValue, value) -> {
                     oldValue.addAll(value);
                     return oldValue;
                 })
        );

        map.forEach((key, value) -> lines.add(Util.commaJoinStrings(key + APPROVED_POSTFIX,
                Collections.min(getCreationDates(value)), Collections.min(getLastModifiedDates(value)))));
         lines.add(0, Util.commaJoinStrings("Name", "Creation Date", "Modified Date"));
        return lines;
    }

    @Override
    public void processReport(IReport report) {
        throw new UnsupportedOperationException();
    }

    private static String getConfigNameByZipName(String zipName) {
        return StringUtils.substringBeforeLast(zipName, DASH);
    }

    private static String getConfigNameByPath(Path path) {
        return getConfigNameByZipName(path.getFileName().toString());
    }

    private static String getCreationDateFromAttribute(BasicFileAttributes attr) {
        return Util.toISODate(attr.creationTime());
    }

    private static String getLastModifiedDateFromAttribute(BasicFileAttributes attr) {
        return Util.toISODate(attr.lastModifiedTime());
    }

    private static BasicFileAttributes getFileAttributes(Path path) {
        try {
            return Files.readAttributes(path, BasicFileAttributes.class);
        } catch (IOException e) {
            return null;
        }
    }

    private static List<String> getCreationDates(List<BasicFileAttributes> attrs) {
         return getISODates(attrs, BookStartDateChecker::getCreationDateFromAttribute);
    }

    private static List<String> getLastModifiedDates(List<BasicFileAttributes> attrs) {
        return getISODates(attrs, BookStartDateChecker::getLastModifiedDateFromAttribute);
    }

    private static List<String> getISODates(List<BasicFileAttributes> attrs, Function<BasicFileAttributes, String> attrMap) {
         return attrs.stream().map(attrMap).collect(Collectors.toList());
    }

    public static void main(String[] args) {
        new BookStartDateChecker().logAndWriteReport();
    }
}

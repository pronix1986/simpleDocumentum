package com.cchis.dctm.util.export.util;

import com.documentum.fc.client.*;
import com.documentum.fc.common.DfException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.cchis.dctm.util.export.util.ExportConstants.*;

public final class Util {

    private static final Logger LOG = Logger.getLogger(Util.class);

    private Util() { }


    public static void shutdownExecutorService(ExecutorService executor, int timeout) {
        if (executor == null) return;
        try {
            executor.shutdown();
            executor.awaitTermination(timeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error(MSG_EXECUTOR_SHUTDOWN_ERROR, e);
        } finally {
            if (!executor.isTerminated()) {
                LOG.error(MSG_EXECUTOR_CANCEL_TASKS);
            }
            executor.shutdownNow();
            LOG.info(MSG_EXECUTOR_SHUTDOWN);
        }
    }

    public static void shutdownExecutorService(ExecutorService executor) {
        shutdownExecutorService(executor, DEFAULT_SHUTDOWN_TIMEOUT);
    }

    public static void closeCollection(IDfCollection collection) {
        if (collection != null) {
            try {
                collection.close();
            } catch (Exception e) {
                LOG.error(MSG_ERROR, e);
            }
        }
    }

    public static String appendDqlHints(String dql, String... hints) {
        String newDql = dql;
        if(hints != null && hints.length > 0) {
            newDql += " enable(" + Util.commaJoinStrings((Object[])hints) + ")";
        }
        return newDql;
    }


    /**
     * There is a method in DctmUtil, but I set DfQuery.DF_READ_QUERY
     * @param session
     * @param dql
     * @return
     */
    public static IDfCollection runQuery(IDfSession session, String dql) throws DfException {
        return runQuery(session, dql, IDfQuery.DF_READ_QUERY);
    }

    public static IDfCollection runQuery(IDfSession session, String dql, int type) throws DfException {
        IDfQuery query = new DfQuery();
        query.setDQL(dql);
        return query.execute(session, type);
    }

    public static int getCount(IDfSession session, String dql, String countAlias) throws DfException {
        IDfCollection collection = null;
        try {
            collection = runQuery(session, dql);
            collection.next();
            return collection.getInt(countAlias);
        } finally {
            closeCollection(collection);
        }
    }

    public static int getCount(IDfSession session, String dql) throws DfException {
        return getCount(session, dql, COUNT_ALIAS);
    }

    public static int getCountByQualification(IDfSession session, String qual) throws DfException {
        return getCount(session, "select count(*) as " + COUNT_ALIAS + " " + qual);
    }

    public static void sleepSilently(int seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException ignore) { }
    }


    public static String simpleArrayToString(String toString, String separator) {
        String result = toString.substring(1, toString.length() - 1).replaceAll("\\s", STR_EMPTY_STRING);
        if (!COMMA.equalsIgnoreCase(separator)) {
            result = result.replaceAll(COMMA, separator);
        }
        return result;
    }

    public static String listOfObjectsToString(List<?> list, String separator) {
        return  list.stream().map(Object::toString).collect(Collectors.joining(separator));
    }

    public static String listOfObjectsToString(List<?> list) {
        if (Util.isNotEmptyCollection(list)) {
            return listOfObjectsToString(list, NEW_LINE);
        }
        return EMPTY_STRING;
    }


    /**
     * Only first level considered
     * @param toString
     * @return
     */
    public static String simpleArrayToStringAsCSV(String toString) {
        return simpleArrayToString(toString, COMMA);
    }

    /**
     * Not fully correct but sufficient for my needs
     * @param toString
     * @return
     */
    public static String mapToStringAsLines(String toString) {
        String result = toString.substring(1, toString.length() - 1)
                .replaceAll("\\s", STR_EMPTY_STRING);
        if(result.contains("[") && result.contains("]")) {
            result = result.replaceAll("],", "]\n");
        } else {
            result = result.replaceAll(COMMA, NEW_LINE);
        }
        return result;
    }

    /**
     * This method is introduced in more recent commons library
     * @param str
     * @param wrap
     * @return
     */
    public static String wrap(final String str, final String wrap) {
        if(StringUtils.isEmpty(str) || StringUtils.isEmpty(wrap)) return str;
        return wrap + str + wrap;
    }

    /**
     * There is joinWith() method but in more recent commons library.
     * @param separator
     * @param strings
     * @return
     */
    public static String joinStrings(String separator, Object... strings) {
        return StringUtils.join(Arrays.asList(strings), separator);
    }

    public static String commaJoinStrings(Object... strings) {
        return joinStrings(COMMA, strings);
    }

    public static String spaceJoinStrings(Object... strings) {
        return joinStrings(SPACE, strings);
    }

    public static String escapeCommaCSV(String input) {
        if (!input.contains(COMMA)) return input;
        return wrap(input, "\"");
    }

    public static String camelCaseToUnderscoreDelimited(String camelCaseStr) {
        return StringUtils.join(StringUtils.splitByCharacterTypeCamelCase(camelCaseStr), UNDERSCORE).toUpperCase();
    }

    public static String trimEnd(String s, String postfix) {
        if (s.endsWith(postfix)) {
            return s.substring(0, s.length() - postfix.length());
        }
        return s;
    }

    /**
     * This method is introduced in more recent commons library
     * @param collection
     * @return
     */
    public static boolean isEmptyCollection(final Collection<?> collection) {
        return collection == null || collection.isEmpty();
    }

    /**
     * This method is introduced in more recent commons library
     * @param collection
     * @return
     */
    public static boolean isNotEmptyCollection(final Collection<?> collection) {
        return !isEmptyCollection(collection);
    }

    /**
     * This method is introduced in more recent commons library
     * @param map
     * @return
     */
    public static boolean isEmptyMap(final Map map) {
        return map == null || map.isEmpty();
    }

    /**
     * This method is introduced in more recent commons library
     * @param map
     * @return
     */
    public static boolean isNotEmptyMap(final Map map) {
        return !isEmptyMap(map);
    }


    /**
     * Similar method is introduced in more recent commons library
     * @param list
     * @param partSize
     * @return
     */
    public static <T> List<List<T>> listPartition(List<T> list, int partSize) {
        if (partSize <= 0 || list == null) {
            throw new IllegalArgumentException("listPartition()");
        }
        List<List<T>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += partSize) {
            partitions.add(list.subList(i, Math.min(i + partSize, list.size())));
        }
        return partitions;
    }

    // as it turned out this method even slower
    public static <T> Collection<List<T>> listPartitionEx(List<T> list, int partSize) {
        if (partSize <= 0 || list == null) {
            throw new IllegalArgumentException("listPartition()");
        }
        final AtomicInteger counter = new AtomicInteger(0);
        return list.stream().collect(Collectors.groupingBy(it -> counter.getAndIncrement() / partSize)).values();
    }


    /**
     * This method is introduced in more recent commons library
     * @param array
     * @return
     */
    public static boolean isNotEmptyArray(Object[] array) {
        return !ArrayUtils.isEmpty(array);
    }

    public static boolean isDateValid(String date, String format, String regex) {
        if (StringUtils.isEmpty(date) || StringUtils.isEmpty(format)) {
            LOG.warn("date and format should be provided");
            return false;
        }

        // for example issue when year < 1000
        if (StringUtils.isNotEmpty(regex) && !date.matches(regex)) return false;

        SimpleDateFormat sdf = new SimpleDateFormat(format);
        sdf.setLenient(false);
        try {
            Date d = sdf.parse(date.trim());
            LOG.trace(String.format("%s:%s", date, d));
        } catch (ParseException e) {
            LOG.warn("isDateValid(): " + e.getMessage());
            return false;
        }
        return true;
    }

    public static boolean isDateValid(String date, String format) {
        return isDateValid(date, format, null);
    }

    public static boolean isSqlDateValid(String date) {
        return isDateValid(date, SQL_DATE_FORMAT, SQL_DATE_REGEX) || isDateValid(date, SQL_DATETIME_FORMAT, SQL_DATETIME_REGEX);
    }

    public static String combineDates(String dateFrom, String dateTo) {
        return joinStrings(PIPE, dateFrom, dateTo);
    }

    public static Map<String, String> getDatesFromFile(File file) throws IOException {
        try (Stream<String> lines = Files.lines(file.toPath())) {
            return lines.map(line -> StringUtils.split(line, COMMA))
                    .collect(Collectors.toMap(args -> args[0], args -> (args.length == 2 ? args[1] : combineDates(args[1], args[2]))));
        }
    }

    public static String[] getFromToDates(String combDate) {
        String[] parsed = StringUtils.split(combDate, PIPE);
        if (parsed.length == 2) {
            return parsed;
        }
        String[] fromToDates = new String[2];
        fromToDates[0] = parsed[0];
        fromToDates[1] = null;
        return fromToDates;
    }

    public static String toISODate(FileTime time) {
        return DateFormatUtils.ISO_DATETIME_FORMAT.format(time.toMillis());
    }

}

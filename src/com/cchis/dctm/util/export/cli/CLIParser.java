package com.cchis.dctm.util.export.cli;

import com.cchis.dctm.util.export.PublishType;
import com.cchis.dctm.util.export.util.FileUtil;
import com.cchis.dctm.util.export.util.Util;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.*;

import static com.cchis.dctm.util.export.util.ExportConstants.*;

/**
 * Available options:
 * -all (not recommended!)
 * -insights
 * -insources (without images)
 * -images
 * -names <space-separated list of names>
 * Any other options will be defaulted to -all
 * Only the first option will be processed, others will be ignored.
 * Case-sensitive. TODO: change to case-insensitive
 *
 * */
public class CLIParser {
    private static final Logger LOG = Logger.getLogger(CLIParser.class);
    private static final int MIN_ARGS = 2;
    private static final String HELP = "/help.txt";
    private static final String USAGE;
    private static final String NAMES = "-names";
    private static final String ALL = "-all";
    private static final String FULL = "-full";
    private static final String DELTA = "-delta";
    private static final String DATE = "-date";
    private static final String DATE_FROM = "-dateFrom";
    private static final String DATE_TO = "-dateTo";
    private static final String FILE = "-file";

    private static final String[] FULL_CONFIG_OPTIONS_LIST = new String[] {NAMES, "-insights", "-insources", "-images", "-all", "-nothing"};

    private static final String AVAILABLE_OPTIONS = "Available options: %s";


    static {
        File help = new File(CLIParser.class.getResource(HELP).getFile());
        USAGE = NEW_LINE + FileUtil.readFileToStringSilently(help, UTF_8);
    }

    private String[] args;

    public CLIParser(String[] args) {
        this.args = Arrays.copyOf(args, args.length);
    }

    public CLIOption parse() {
        int position = 0;
        if (args.length < MIN_ARGS) {
            help("Number of provided arguments < " + MIN_ARGS);
        }
        String publishType = args[position++];
        if (FULL.equalsIgnoreCase(publishType)) {
            String dql = parseConfigArgs(1, null);
            LOG.debug(String.format(MSG_WEBC_DQL, dql));
            return new CLIOption(PublishType.FULL, dql, null);

        } else if (DELTA.equalsIgnoreCase(publishType)) {
            String date = EMPTY_STRING;
            Map<String, String> dates = new HashMap<>();
            String dateSource = args[position++];
            if (DATE.equalsIgnoreCase(dateSource)) {
                if (args.length == MIN_ARGS) help(DATE + " should not be empty");
                String dateToParse = args[position++];
                if (Util.isSqlDateValid(dateToParse)) {
                    date = dateToParse;
                } else {
                    help(String.format("%s is not valid: %s", DATE, dateToParse));
                }
            } else if (DATE_FROM.equalsIgnoreCase(dateSource)) {
                if (args.length < 5) help(String.format("Usage: %s <date1> %s <date2>", DATE_FROM, DATE_TO));
                String dateFrom = args[position++];
                if (!DATE_TO.equalsIgnoreCase(args[position++])) help(DATE_TO + " required");
                String dateTo = args[position++];
                if (!Util.isSqlDateValid(dateFrom) || !Util.isSqlDateValid(dateTo)) {
                    help(String.format("%s or %s not valid: %s %s", DATE_FROM, DATE_TO, dateFrom, dateTo));
                } else {
                    date = Util.combineDates(dateFrom, dateTo);
                }
            } else if (FILE.equalsIgnoreCase(dateSource)) {
                if (args.length == MIN_ARGS) help(FILE + " should not be empty");
                try {
                    String filePath = args[position++];
                    File file = new File(FilenameUtils.separatorsToSystem(filePath));
                    dates.putAll(Util.getDatesFromFile(file));
                } catch (Exception e) {
                    help("Error reading file");
                }
            } else {
                help(String.format(AVAILABLE_OPTIONS, Util.spaceJoinStrings(DATE, DATE_FROM, FILE)));
            }
            String dql = parseConfigArgs(position, ALL, NAMES, ALL);
            LOG.debug(String.format(MSG_WEBC_DQL, dql));
            dates.put(EMPTY_STRING, date);
            return new CLIOption(PublishType.DELTA, dql, dates);
        } else {
            help(String.format(AVAILABLE_OPTIONS, Util.spaceJoinStrings(FULL, DELTA)));
        }
        help();
        return null;
    }

    private void help(String reason) {
        if (StringUtils.isNotEmpty(reason)) {
            LOG.warn(reason);
        }
        LOG.info(USAGE);
        System.exit(0);
    }

    private void help() {
        help(null);
    }

    private String generateConfigDql(String[] nameListArgs, String... excludeNameList) {
        if (nameListArgs.length == 0) {
            help("name_list should not be empty");
        }
        List<String> names = new ArrayList<>();
        int i = 0;
        while (i < nameListArgs.length && !nameListArgs[i].startsWith("-")) {
            String name = nameListArgs[i++];
            names.add(name);
        }
        if (names.isEmpty()) {
            help("name_list is invalid: " + Arrays.asList(nameListArgs));
        }
        StringBuilder builder = new StringBuilder(DQL_GET_ALL_SCS_CONFIGS_PREFIX);
        for (int j = 0; j < names.size(); j++) {
            if (j != 0) builder.append(" OR ");
            builder.append(String.format(DQL_GET_ALL_SCS_CONFIGS_NAME_PATTERN, names.get(j)));
        }
        if (Util.isNotEmptyArray(excludeNameList)) {
            builder.append(") AND (");
            for (int j = 0; j < excludeNameList.length; j++) {
                if(j != 0) builder.append(" AND ");
                builder.append(String.format(DQL_GET_ALL_SCS_CONFIGS_EXCLUDE_NAME_PATTERN, excludeNameList[j]));
            }
        }
        builder.append(DQL_GET_ALL_SCS_CONFIGS_POSTFIX);
        return builder.toString();
    }

    private String generateConfigDql(int startPosition) {
        String[] nameList = new String[args.length - startPosition];
        System.arraycopy(args, startPosition, nameList, 0, nameList.length);
        return generateConfigDql(nameList);
    }

    private String parseConfigArgs(int startPosition, String defaultOption, String... availableOptions) {
        String configOption = startPosition >= args.length ? defaultOption : args[startPosition];
        if(availableOptions.length == 0) {
            availableOptions = FULL_CONFIG_OPTIONS_LIST;
        }
        if(Arrays.asList(availableOptions).contains(configOption) ) {
            if (ALL.equalsIgnoreCase(configOption)) {
                return generateConfigDql(new String[] {ROOT_IMAGE_CONFIG_NAME, "__-INS"});
            }
            if ("-insights".equalsIgnoreCase(configOption)) {
                return generateConfigDql(new String[] {"__-INSIGHT"});
            }
            if ("-insources".equalsIgnoreCase(configOption)) {
                return generateConfigDql(new String[] {"__-INSOURCE"}, "__-INSOURCE-IMAGES");
            }
            if ("-images".equalsIgnoreCase(configOption)) {
                return generateConfigDql(new String[] {ROOT_IMAGE_CONFIG_NAME, "__-INSOURCE-IMAGES"});
            }
            if (NAMES.equalsIgnoreCase(configOption)) {
                return generateConfigDql(++startPosition);
            }
            // for test
            if ("-nothing".equalsIgnoreCase(configOption)) {
                return DQL_GET_ZERO_SCS_CONFIGS;
            }
        }
        help(String.format(AVAILABLE_OPTIONS, Util.simpleArrayToString(ArrayUtils.toString(availableOptions), " ")));
        return EMPTY_STRING;
    }
}

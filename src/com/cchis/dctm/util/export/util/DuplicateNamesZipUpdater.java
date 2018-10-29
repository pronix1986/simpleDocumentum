package com.cchis.dctm.util.export.util;

import com.cchis.dctm.util.export.*;
import com.cchis.dctm.util.export.report.ReportRecord;
import com.documentum.admin.object.IDfWebCacheConfig;
import com.documentum.fc.client.IDfSession;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.nio.file.*;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.cchis.dctm.util.export.report.DuplicateNamesReport.DUP_NAME_PATH;
import static com.cchis.dctm.util.export.util.ExportConstants.*;
import static com.cchis.dctm.util.export.util.ExportConstants.DUPLICATE_NAME_POSTFIX;
import static com.cchis.dctm.util.export.util.ExportConstants.ZIP_EXT;

/**
 * Issue with Duplicate Names. Sometimes both of the records are in properties.xml but another time only one of them.
 * This is core Documentum issue that I cannot handle directly.
 * Here I recreated 'duplicate names' zip archives but instead of reusing publishing, I create properties.xml file from scratch.
 * Old zip archives are backed. New zip archives are copied in NEW_DUP_NAMES_DIR directory.
 */
public class DuplicateNamesZipUpdater {
    private static final Logger LOG = Logger.getLogger(DuplicateNamesZipUpdater.class);
    private static final String BACKUP_POSTFIX = "_bak";
    private static final String NEW_DUP_NAMES_DIR = "Export_NEW_DUP_NAMES";

    private static final CopyOption[] COPY_OPTIONS = {
            StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES
    };

    private DuplicateNamesZipUpdater() { }

    public static void update() {
        File root = new File(EXPORT_DIR_PATH);
        String postfix = DUPLICATE_NAME + ZIP_EXT;

        final IDfSession aSession = SessionManagerHandler.getInstance().getMainSession();

        // backup original zip files
        Predicate<Path> backupFilter = path -> {
            String origPathStr = path.toString();
            String backupPathStr = Util.trimEnd(origPathStr, ZIP_EXT) + BACKUP_POSTFIX + ZIP_EXT;
            Path backup = Paths.get(backupPathStr);
            return FileUtil.copyFileSilently(path, backup, COPY_OPTIONS);
        };
        // prefixed name
        Function<ReportRecord, String> nameMap = record -> {
            String docName = record.getFilteredObjectNameNullSafe();
            String id = record.getId();
            return id + UNDERSCORE + docName;
        };
        // extract ids from name
        Function<String, String> idExtractor = name -> StringUtils.substringBefore(StringUtils.substringAfterLast(name, "\\"), "_");
        // create new zip archives
        BiConsumer<IDfWebCacheConfig, List<String>> exportConsumer =
                (config, ids) -> {
                    ConfigId cId = ConfigId.getConfigIdFailSafe(config);
                    String exportPathStr = DUP_NAME_PATH + File.separator + EXPORT_SET_FOLDER_PREFIX + cId.getConfigId().substring(8);
                    Path exportPath = Paths.get(exportPathStr);
                    String fullZipFileName = WebcHandler.createNameForConfigWithVersion(cId.getConfigName(), null, null,
                            DUPLICATE_NAME_POSTFIX) + ZIP_EXT;
                    Path zipPath = new File(WebcHandler.getZipFolder(cId.getConfigName()), fullZipFileName).toPath();

                    ExportResult exportResult = FileUtil.exportAndZipFilesUnderBook(config, ids, nameMap, exportPath, zipPath);
                    int count = exportResult.getZipCount();
                    if (count != ids.size()) {
                        LOG.warn(String.format("count != ids.size(): %s %s", count, ids.size()));
                    }

                    Path copy = Paths.get(zipPath.toAbsolutePath().toString().replace("Export", NEW_DUP_NAMES_DIR));
                    FileUtil.copyFileSilently(zipPath, copy, COPY_OPTIONS);
                };
        Function<Path, IDfWebCacheConfig> getConfigByPath = path -> WebcHandler.getConfigByPartName(aSession, Util.trimEnd(path.getFileName().toString(), postfix));
        Function<Path, List<String>> getIdsByPath = path -> FileUtil.getZipFileNames(path.toFile()).stream().filter(name -> name.contains("content_dir"))
                .map(idExtractor).collect(Collectors.toList());

        FileUtil.findPathsByPartName(root, postfix).filter(backupFilter)
                .collect(Collectors.toMap(getConfigByPath, getIdsByPath)).forEach(exportConsumer);
        LOG.info("DuplicateNamesZipUpdater: done");
    }

    public static void main(String[] args) {
        DuplicateNamesZipUpdater.update();
    }
}

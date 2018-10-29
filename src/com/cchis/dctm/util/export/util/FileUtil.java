package com.cchis.dctm.util.export.util;

import com.cchis.dctm.util.export.*;
import com.cchis.dctm.util.export.exception.ExportException;
import com.cchis.dctm.util.export.report.ConfigReport;
import com.cchis.dctm.util.export.report.DuplicateNamesReport;
import com.cchis.dctm.util.export.report.ReportRecord;
import com.documentum.admin.object.IDfWebCacheConfig;
import com.documentum.com.IDfClientX;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfId;
import com.documentum.fc.common.IDfId;
import com.documentum.fc.common.IDfList;
import com.documentum.operations.IDfExportNode;
import com.documentum.operations.IDfExportOperation;
import com.documentum.operations.IDfOperationError;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import static com.cchis.dctm.util.export.util.ExportConstants.*;
import static com.cchis.dctm.util.export.util.ExportConstants.MSG_ERROR_DELETE_SCS_DIR;
import static com.cchis.dctm.util.export.util.ExportConstants.PROPERTIES_XML;

public final class FileUtil {

    private static final Logger LOG = Logger.getLogger(FileUtil.class);
    /**
     * may have documentum 'trash' files like rep.sync or <name>.ids
     */
    private static final List<String> BLACKLIST = new ArrayList<>();
    /**
     * we may also have for example original zip archive transmitting to target at the moment of zipping ('fast' job issue)
     */
    private static final List<String> WHITELIST = new ArrayList<>();

    static {
        WHITELIST.add(CONTENT_DIR);
        WHITELIST.add(PROPERTIES_XML);
        BLACKLIST.add(".sync");
        BLACKLIST.add(".ids");
        BLACKLIST.add(ZIP_EXT);
    }

    private FileUtil() { }

    /**
     * Documentum may leave 'trash' documents in export set and I don't know all the cases.
     * That is why I need for both BLACKLIST and WHITELIST.
     * But it may not guarantee correct result though.
     * @param fileName
     * @return
     */
    private static boolean shouldIgnore(String fileName) {
        Objects.requireNonNull(fileName);
        for (String i : BLACKLIST) {
            if (fileName.contains(i)) {
                return true;
            }
        }
        for (String i : WHITELIST) {
            if (fileName.contains(i)) {
                return false;
            }
        }
        return true;
    }

    private static boolean shouldExcludeFromCount(String fileName) {
        return PROPERTIES_XML.equalsIgnoreCase(fileName);
    }

    private static int zipFile(File fileToZip, String fileName, ZipOutputStream zipOut) throws IOException {
        int count = 0;
        if (fileToZip.isHidden() || shouldIgnore(fileName)) {
            return count;
        }
        if (fileToZip.isDirectory()) {
            File[] children = fileToZip.listFiles();
            if (children != null) {
                for (File childFile : children) {
                    count += zipFile(childFile, fileName + File.separator + childFile.getName(), zipOut);
                }
            }
            return count;
        }
        if (!shouldExcludeFromCount(fileName)) {
            count++;
        }

        ZipEntry zipEntry = new ZipEntry(fileName);
        zipOut.putNextEntry(zipEntry);
        zipOut.write(Files.readAllBytes(fileToZip.toPath()));
        zipOut.closeEntry();

        return count;
    }

    /**
     * Returns number of 'white-list' files
     * @param dir
     * @param zipOut
     * @return
     * @throws IOException
     */
    public static int zipRootDirectory(File dir, ZipOutputStream zipOut) throws IOException {
        logIfInvalidDirectory(dir);
        int count = 0;
        File[] children = dir.listFiles();
        if (children != null) {
            for (File childFile : children) {
                count += zipFile(childFile, childFile.getName(), zipOut);
            }
        }
        return count;

    }

    public static boolean deleteDirectory(File dir) {
        boolean result = true;
        try {
            FileUtils.deleteDirectory(dir);
        } catch (Exception e) {
            LOG.debug(String.format(MSG_ERROR_DELETE_DIR, dir.getName(), e.getMessage()));
            result = false;
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public static List<String> getExportSetFiles(File root, String basePath) {
        logIfInvalidDirectory(root);
        List<String> names = new ArrayList<>();
        Collection<File> files = FileUtils.listFiles(root, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
        for (File f : files) {
            String path = f.getAbsolutePath();
            int index = path.indexOf(CONTENT_DIR);
            if (index > 0) {
                names.add(basePath + FilenameUtils.separatorsToUnix(path.substring(index + CONTENT_DIR.length())));
            }
        }
        return names;
    }

    public static List<String> getExportSetsFiles(List<File> roots, String basePath) {
        List<String> names = new ArrayList<>();
        roots.forEach(root -> names.addAll(getExportSetFiles(root, basePath)));
        return names;
    }

    public static Stream<Path> findPaths(File root, Predicate<Path> pathPredicate) throws IOException {
        return Files.walk(root.toPath()).filter(pathPredicate);
    }

    public static Stream<Path> findPathsByPartName(File root, String partName)  {
        Predicate<Path> pathNamePredicate = path -> path.getFileName().toString().contains(partName);
        try {
            return findPaths(root, pathNamePredicate);
        } catch (Exception e) {
            throw new ExportException("findPathsByPartName()");
        }
    }

    public static List<String> getZipFileNames(File zipFile) {
        if (zipFile.exists()) {
            try (ZipFile zip = new ZipFile(zipFile)) {
                return zip.stream().map(ZipEntry::getName).collect(Collectors.toList());
            } catch (IOException e) {
                LogHandler.logWithDetails(LOG, MSG_ERROR, e);
            }
        } else {
            LOG.warn("File does not exist: " + zipFile);
        }
        throw new ExportException("getZipFileNames()");
    }

    private static boolean isExportSetValid(File exportSet) {
        if (exportSet == null || !isValidDirectory(exportSet)) {
            return false;
        }
        List<String> filenames = Arrays.stream(exportSet.listFiles()).map(File::getName).collect(Collectors.toList());
        return filenames.contains(CONTENT_DIR) && filenames.contains(PROPERTIES_XML);
    }

    public static File getValidExportSet(List<File> exportSets) {
        ListIterator<File> itr = exportSets.listIterator(exportSets.size());
        while (itr.hasPrevious()) {     // Why reverse order? Weird issue with 'fast' config
            File exportSet = itr.previous();
            if (isExportSetValid(exportSet)) return exportSet;
        }
        throw new ExportException("Couldn't find valid export set among: " + exportSets);
    }

    public static List<File> getValidExportSets(List<File> exportSets)  {
        Set<File> eSet = new TreeSet<>(Comparator.comparing(File::getName));
        ListIterator<File> itr = exportSets.listIterator(exportSets.size());
        while (itr.hasPrevious()) {     // Why reverse order? Weird issue with 'fast' config
            File exportSet = itr.previous();
            if (isExportSetValid(exportSet)) eSet.add(exportSet);
        }
        if (Util.isEmptyCollection(eSet)) {
            throw new ExportException("Couldn't find valid export set among: " + exportSets);
        } else {
            return new ArrayList<>(eSet);
        }
    }

    public static boolean isDirEmpty(File dir) {
        return dir != null && dir.isDirectory() && dir.list() != null && dir.list().length == 0;
    }

    public static void mkdirs(File file) {
        Objects.requireNonNull(file);
        if (!file.exists() && !file.mkdirs()) {
            LOG.trace("Cannot create directory or directory already exists: " + file.getName());
        }
    }

    public static void mkdirs(Path path) {
        Objects.requireNonNull(path);
        File file = path.toFile();
        if (isValidDirectory(file, false)) {
            mkdirs(file);
        } else {
            mkdirs(file.getParentFile());
        }
    }

    public static void deleteTargetRootDir() {
        deleteDirAndLog(TARGET_ROOT_DIR_PATH, MSG_DELETE_SCS_DIR_SUCCESS, MSG_ERROR_DELETE_SCS_DIR);
    }

    public static void deleteBatchDir() {
        deleteDirAndLog(BATCH_TEMP_DIR_PATH, MSG_DELETE_BATCH_DIR_SUCCESS, MSG_ERROR_DELETE_BATCH_DIR);
    }

    public static void deleteDupNamesDir() {
        deleteDirAndLog(DuplicateNamesReport.DUP_NAME_PATH, MSG_DELETE_DUP_NAMES_DIR_SUCCESS, MSG_ERROR_DELETE_DUP_NAMES_DIR);
    }

    public static void finalDeleteDirs() {
        deleteTargetRootDir();
        deleteBatchDir();
        deleteDupNamesDir();
    }

    public static void deleteDirAndLog(String path, String successMsg, String errorMsg) {
        File dir = new File(path);
        if (deleteDirectory(dir)) {
            LOG.trace(successMsg);
        } else {
            LOG.warn(errorMsg);
        }
    }

    public static void cleanDir(File dir) {
        if (dir.exists()) {
            try {
                FileUtils.cleanDirectory(dir);
            } catch (Exception e) {
                LOG.debug(String.format(MSG_ERROR_CLEAN_DIR, dir.getName(), e.getMessage()));
            }
        } else {
            LOG.debug("cleanDir(). Directory does not exists: " + dir.getAbsolutePath());
        }
    }

    public static void cleanTargetRootDir() {
        File dir = new File(TARGET_ROOT_DIR_PATH);
        cleanDir(dir);
    }

    public static void cleanResponseDir(List<String> ids) {
        Objects.requireNonNull(IDS_RESPONCE_DIR_FILE);
        Arrays.stream(IDS_RESPONCE_DIR_FILE.listFiles()).filter(f -> {
            String id = findStringByKeyEx(f, "Config Object Id");
            return StringUtils.isNotEmpty(id) && ids.contains(id);
        }).forEach(FileUtil::deleteQuietly);
    }

    private static boolean isValidDirectory(File dir) {
        return isValidDirectory(dir, true);
    }

    private static boolean isValidDirectory(File dir, boolean shouldLog) {
        if (dir.exists() && !dir.isHidden() && dir.isDirectory() && dir.canRead() && dir.canWrite()) {
            return true;
        }
        if (shouldLog) {
            Util.sleepSilently(DEFAULT_INTERVAL);
            LOG.warn(String.format("isValidDirectory(). Problem with dir: %s. Directory?: %s. "
                            + "File?: %s. Hidden?: %s. Exists?: %s. Can Read/Write?: %s/%s ",
                    dir.getAbsolutePath(), dir.isDirectory(),  dir.isFile(), dir.isHidden(),
                    dir.exists(), dir.canRead(), dir.canWrite()));
        }
        return false;
    }

    private static void logIfInvalidDirectory(File dir) {
        isValidDirectory(dir);
    }

    public static void copyDirectorySilently(File source, File destination) {
        try {
            mkdirs(destination.getParentFile());
            FileUtils.copyDirectory(source, destination);
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, Level.WARN, "Cannot copy file", e);
        }
    }

    public static boolean copyFileSilently(Path source, Path destination, CopyOption... options) {
        FileUtil.mkdirs(destination);
        try {
            Files.copy(source, destination, options);
            return true;
        } catch (IOException e) {
            LogHandler.logWithDetails(LOG, "Cannot copy to new destination: " + destination.toString(), e);
            return false;
        }
    }

    public static boolean writeStringToFileSilently(File file, String data) {
        try {
            mkdirs(file.getParentFile());
            FileUtils.writeStringToFile(file, data, UTF_8);
            return true;
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, Level.WARN, "Cannot write String to file", e);
            return false;
        }
    }

    public static boolean writeLinesSilently(File file, Collection<?> data) {
        try {
            mkdirs(file.getParentFile());
            FileUtils.writeLines(file, UTF_8, data);
            return true;
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, Level.WARN, "Cannot write lines to file", e);
            return false;
        }
    }

    public static String readFileToStringSilently(File file, String encoding) {
        try {
            return FileUtils.readFileToString(file, encoding);
        } catch (Exception e) {
            LogHandler.logWithDetails(LOG, Level.WARN, "Cannot read file to string", e);
            return EMPTY_STRING;
        }
    }

    public static void copyInputStreamToFile(final InputStream source, final File destination) throws IOException {
        try {
            Files.copy(source, destination.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } finally {
            IOUtils.closeQuietly(source);
        }
    }

    /**
     * This method is introduced in more recent commons library
     * @param file
     * @return
     */
    public static boolean deleteQuietly(final File file) {
        if (file == null) return false;
        if (file.isDirectory()) {
            cleanDir(file);
        }
        try {
            return file.delete();
        } catch (Exception ignored) {
            return false;
        }
    }

    public static String findStringByKey(InputStream is, String key) throws IOException {
        String regex = key + "\\s*" + COLON;
        Pattern pattern = Pattern.compile(regex);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(is, UTF_8_CS))) {
            String line;
            while ((line = br.readLine()) != null) {
                Matcher matcher = pattern.matcher(line);
                if (matcher.find()) {
                    return line.substring(matcher.end()).trim();
                }
            }
        }
        return null;
    }

    /**
     * Find a line with the following structure: 'key: value' and return value by key.
     * @param file
     * @param key
     * @return
     */
    public static String findStringByKeyEx(File file, String key) {
        String regex = key + "\\s*" + COLON;
        Pattern pattern = Pattern.compile(regex);
        try (BufferedReader br = Files.newBufferedReader(file.toPath())) {
            String line;
            while ((line = br.readLine()) != null) {
                Matcher matcher = pattern.matcher(line);
                if (matcher.find()) {
                    return line.substring(matcher.end()).trim();
                }
            }
        } catch (NoSuchFileException nsfe) {
            LOG.debug("findStringByEx(). File does not exist now: " + file); // debug level because currently this method is used by cleanup thread only
        } catch (IOException e) {
            LogHandler.logWithDetails(LOG, Level.WARN, "findStringByKeyEx()", e);
        }
        return null;
    }

    /**
     * Little modification of DctmUtil.exportContentFile
     */
    public static IDfExportNode exportContentFile(IDfSysObject doc, File expDir, String newName) throws DfException {
        mkdirs(expDir);
        String exportDirectory = expDir.getAbsolutePath();
        IDfClientX clientX = SessionManagerHandler.getInstance().getClientX();
        IDfExportOperation operation = clientX.getExportOperation();
        //operation.setDestinationDirectory(exportDirectory);
        IDfExportNode node = (IDfExportNode) operation.add(doc);
        if (node != null) {
            String objectName = StringUtils.isNotEmpty(newName) ? newName : doc.getObjectName();
            String filePath = exportDirectory + File.separator + objectName;
            LOG.trace("exportContentFile(). filePath: " + filePath);
            node.setFilePath(filePath);
            if (operation.execute()) {
                return node;
            } else {
                StringBuilder errorStringBuilder = new StringBuilder();
                IDfList errorList = operation.getErrors ();
                for ( int i = 0; i < errorList.getCount (); i++ ) {
                    errorStringBuilder.append(((IDfOperationError)errorList.get(i)).getException().getStackTraceAsString());
                }
                String errorString = errorStringBuilder.toString();
                LOG.trace(errorString);
                throw new DfException("Error exporting file: " + doc.getObjectId().getId() + " : " + errorString);
            }
        }
        throw new DfException("Error adding ad node: " + doc.getObjectId().getId());
    }

    /**
     * Returns list of 'ready-to-import' records
     * @param config
     * @param ids
     * @param nameMap
     * @param rootPath
     * @return
     * @throws DfException
     */
    public static ExportResult exportFilesUnderBook(IDfWebCacheConfig config,
                                                  List<String> ids, Function<ReportRecord, String> nameMap,
                                                  Path rootPath) throws DfException {

        if (Util.isEmptyCollection(ids)) {
            ids = new ArrayList<>(DocumentHandler.getModifiedRecordsIdsUnderBookWithVersion(config, null));
        }

        IDfSession session = config.getSession();
        String sourceFolderPath = WebcHandler.getPublishFolder(config);
        ConfigId cId = ConfigId.getConfigIdFailSafe(config);
        String rootPathStr = rootPath.toAbsolutePath().toString();
        File exportSet = rootPath.toFile();
        String base = rootPathStr + File.separator + CONTENT_DIR;
        File baseFile = new File(base);
        FileUtil.mkdirs(baseFile.getParentFile());

        List<ReportRecord> allDupNamesRecords = new ArrayList<>();
        List<ReportRecord> currentReadyToImport = new ArrayList<>();

        for (String id : ids) {
            IDfId dfId = new DfId(id);
            IDfSysObject document = (IDfSysObject) session.getObject(dfId);
            ReportRecord record = new ReportRecord(session, dfId);
            ConfigReport.filterRecordPath(cId, record);
            allDupNamesRecords.add(record);
            if (record.isReadyToImport()) {
                currentReadyToImport.add(record);
            }
            String relativeFolderPath = DocumentHandler.getFolderName(document).substring(sourceFolderPath.length());
            String fullPath = base + FilenameUtils.separatorsToSystem(relativeFolderPath);
            IDfExportNode node = FileUtil.exportContentFile(document, new File(fullPath), nameMap.apply(record));
            LOG.trace("filepath: " + node.getFilePath());
        }

        File propertiesXml = new File(exportSet, PROPERTIES_XML);
        PropertiesXMLGenerator generator = new PropertiesXMLGenerator(config, allDupNamesRecords, propertiesXml);
        generator.writeXML();

        return new ExportResult(allDupNamesRecords, currentReadyToImport, 0);
    }

    public static ExportResult exportAndZipFilesUnderBook(IDfWebCacheConfig config,
                List<String> ids, Function<ReportRecord, String> nameMap, Path exportRootPath, Path zipPath) {

        File exportSet = exportRootPath.toFile();

        ExportResult exportResult = new ExportResult();
        int count = 0;
        try(ZipOutputStream out = new ZipOutputStream(Files.newOutputStream(zipPath))) {
            exportResult = FileUtil.exportFilesUnderBook(config, ids, nameMap, exportRootPath);
            count = FileUtil.zipRootDirectory(exportSet, out);
            LOG.debug(String.format(MSG_ZIP_CREATED, zipPath.getFileName().toString()));
        } catch (Exception e) {
            LOG.warn("Cannot create zip for duplicate names", e);
        }

        exportResult.setZipCount(count);
        return exportResult;
    }
}

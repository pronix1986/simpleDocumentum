package com.cchis.dctm.util.export.util;

import com.cchis.dctm.util.export.WebcHandler;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static com.cchis.dctm.util.export.util.ExportConstants.EXPORT_DIR_PATH;
import static com.cchis.dctm.util.export.util.ExportConstants.MSG_ZIP_CREATED;


/**
 *  Used to create zip files containing no more than specified number of records.
 *  The only real case I can think is for root Images with 1.0 version.
 *  UPDATE: not in use currently since Dzmitry implement 'image splitter' functionality on his side
 */
public class ChunkedZipOutputStream extends ZipOutputStream {
    private static final Logger LOG = Logger.getLogger(ChunkedZipOutputStream.class);

//    private static final int MAX_ENTRIES_COUNT = 5000; // externalize?
    private static final int MAX_ENTRIES_COUNT = Integer.MAX_VALUE; // removed this functionality

    private static final String PART_POSTFIX = ".part.";
    private static final String FILE_EXTENSION = ".zip";
    private static final String DUMMY_ENTRY = "dummy_entry";

    private ZipOutputStream zipOutputStream;
    private int currentCount = 0;
    private int currentChunkIndex = 1;
    private String zipName;
    private boolean isFirstPart = true;


    public ChunkedZipOutputStream(String zipName) throws IOException {
        super(new NullOutputStream());
        super.putNextEntry(new ZipEntry(DUMMY_ENTRY)); // even "dummy" zip should have at least one entry
        this.zipName = zipName;
        openNewStream();
        isFirstPart = false;
    }

    private void closeStream() throws IOException {
        zipOutputStream.close();
    }

    @Override
    public void close() throws IOException {
        closeStream();
        super.close();
    }

    private void openNewStream() throws FileNotFoundException {
        String absolutePath = getPartName();
        zipOutputStream = new ZipOutputStream(
                new FileOutputStream(new File(absolutePath)));
        currentChunkIndex++;
        currentCount = 0;
    }


    public String getPartName() {
        StringBuilder builder = new StringBuilder(EXPORT_DIR_PATH);
        builder.append(File.separator);
        builder.append(zipName);
        if (!isFirstPart) {
            builder.append(PART_POSTFIX);
            builder.append(currentChunkIndex);
        }
        builder.append(FILE_EXTENSION);
        return builder.toString();
    }

    public String getZipName() {
        return zipName;
    }

    @Override
    public void putNextEntry(ZipEntry e) throws IOException {
        if (currentCount >= MAX_ENTRIES_COUNT) {
            closeStream();
            LOG.debug(String.format(MSG_ZIP_CREATED, getPartName()));
            openNewStream();
        }
        currentCount++;
        zipOutputStream.putNextEntry(e);
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        zipOutputStream.write(b, off, len);
    }

    public int getCurrentCount() {
        return currentCount;
    }
}


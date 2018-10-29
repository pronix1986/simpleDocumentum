package com.cchis.dctm.util.export.util;

import com.cchis.dctm.util.export.exception.ExportException;
import org.apache.log4j.Logger;

import static com.cchis.dctm.util.export.util.ExportConstants.MSG_ERROR;

public class ThrowExceptionUtil {
    private static final Logger LOG = Logger.getLogger(ThrowExceptionUtil.class);

    public static void main(String[] args) {

        try {
            throw new ExportException("TEST");
        } catch (ExportException e) {
            LOG.error(MSG_ERROR, e);
        }

    }
}

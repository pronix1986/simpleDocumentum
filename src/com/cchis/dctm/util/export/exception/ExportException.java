package com.cchis.dctm.util.export.exception;

public class ExportException extends RuntimeException {
    public ExportException() {
    }

    public ExportException(String message) {
        super(message);
    }

    public ExportException(String message, Throwable cause) {
        super(message, cause);
    }

    public ExportException(Throwable cause) {
        super(cause);
    }
}

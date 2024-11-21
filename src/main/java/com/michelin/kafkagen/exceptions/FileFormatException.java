package com.michelin.kafkagen.exceptions;

public class FileFormatException extends RuntimeException {
    public FileFormatException(String path) {
        super(String.format("Unable to read the provided file %s. Please check the path and content (text only)",
            path));
    }
}

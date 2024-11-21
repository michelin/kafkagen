package com.michelin.kafkagen.models;

import lombok.Getter;

@Getter
public enum CompactedAssertState {
    FOUND("Record found"),
    NOT_FOUND("Record not found"),
    NEWER_VERSION_EXISTS("Record was found but a newer record with the same key exists");

    private final String message;

    CompactedAssertState(String message) {
        this.message = message;
    }
}

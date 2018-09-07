package ru.curs.celesta.exception;

import ru.curs.celesta.CelestaException;

public class CelestaParseException extends CelestaException {

    public CelestaParseException(Throwable cause) {
        super(cause);
    }

    public CelestaParseException(String message, Object... args) {
        super(message, args);
    }
}

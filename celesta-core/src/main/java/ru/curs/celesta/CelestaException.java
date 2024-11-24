package ru.curs.celesta;

/**
 * Critical error that leads to stopping of Celesta.
 */
public class CelestaException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public CelestaException(String message) {
        super(message);
    }

    public CelestaException(String message, Object... args) {
        super(String.format(message, args));
    }

    public CelestaException(String message, Throwable cause) {
        super(message, cause);
    }

    public CelestaException(Throwable cause) {
        super(cause);
    }

}

package ru.curs.celesta;

/**
 * Критическая ошибка, приводящая к остановке Celesta.
 * 
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

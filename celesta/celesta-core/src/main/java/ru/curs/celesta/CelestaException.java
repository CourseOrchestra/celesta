package ru.curs.celesta;

/**
 * Критическая ошибка, приводящая к остановке Celesta.
 * 
 */
public class CelestaException extends Exception {

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
}

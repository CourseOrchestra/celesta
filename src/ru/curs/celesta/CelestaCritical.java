package ru.curs.celesta;

/**
 * Критическая ошибка, приводящая к остановке Celesta.
 * 
 */
public class CelestaCritical extends Exception {

	private static final long serialVersionUID = 1L;

	public CelestaCritical(String message) {
		super(message);
	}
}

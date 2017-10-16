package ru.curs.lyra.grid;

/**
 * 'Unknown character' error.
 */
class LyraCollatorException extends Exception {

	private static final long serialVersionUID = 1L;

	private final char unknownChar;

	LyraCollatorException(char c) {
		unknownChar = c;
	}

	char getUnknownChar() {
		return unknownChar;
	}

}

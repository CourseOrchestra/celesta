package ru.curs.lyra;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ru.curs.celesta.CelestaException;

/**
 * Lyra named element (field or field value). This class ensures that name is a
 * valid itentifier.
 * 
 * This class is valid for RPC use.
 */
public abstract class LyraNamedElement {
	/**
	 * Максимальная длина идентификатора Celesta.
	 */
	private static final int MAX_IDENTIFIER_LENGTH = 30;
	private static final Pattern NAME_PATTERN = Pattern.compile("[a-zA-Z_][0-9a-zA-Z_]*");

	private final String name;

	public LyraNamedElement(String name) throws CelestaException {
		if (name == null)
			throw new IllegalArgumentException();
		validateIdentifier(name);
		this.name = name;
	}

	static void validateIdentifier(String name) throws CelestaException {
		Matcher m = NAME_PATTERN.matcher(name);
		if (!m.matches())
			throw new CelestaException("Invalid identifier: '" + name + "'.");
		if (name.length() > MAX_IDENTIFIER_LENGTH)
			throw new CelestaException(
					"Identifier '" + name + "' is longer than " + MAX_IDENTIFIER_LENGTH + " characters.");

	}

	/**
	 * Returns name.
	 */
	public final String getName() {
		return name;
	}

	@Override
	public final int hashCode() {
		return name.hashCode();
	}

	@Override
	public final boolean equals(Object obj) {
		return obj instanceof LyraNamedElement ? name.equals(((LyraNamedElement) obj).getName()) : name.equals(obj);
	}
}
package ru.curs.celesta.score;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Именованный элемент метамодели (например, таблица или колонка), который
 * должен иметь уникальное имя-идентификатор.
 * 
 */
public abstract class NamedElement {

	/**
	 * Максимальная длина идентификатора Celesta.
	 */
	private static final int MAX_IDENTIFIER_LENGTH = 30;

	private static final Pattern COMMENT = Pattern.compile("/\\*\\*(.*)\\*/", Pattern.DOTALL);
	private static final Pattern NAME_PATTERN = Pattern.compile("[a-zA-Z_][0-9a-zA-Z_]*");

	private final String name;
	private final String quotedName;

	private String celestaDoc;

	public NamedElement(String name) throws ParseException {
		// Не должно быть name==null, т. к. все методы написаны исходя из того,
		// что name != null.
		if (name == null)
			throw new IllegalArgumentException();
		validateIdentifier(name);
		this.name = name;
		this.quotedName = String.format("\"%s\"", name);
	}

	static void validateIdentifier(String name) throws ParseException {
		Matcher m = NAME_PATTERN.matcher(name);
		if (!m.matches())
			throw new ParseException(String.format("Invalid identifier: '%s'.", name));
		if (name.length() > MAX_IDENTIFIER_LENGTH)
			throw new ParseException(
					String.format("Identifier '%s' is longer than %d characters.", name, MAX_IDENTIFIER_LENGTH));
	}

	/**
	 * Ограничивает длину идентификатора максимальным числом символов.
	 * 
	 * @param value
	 *            Идентификатор произвольной длины.
	 * @return "Подрезанный" идентификатор, последние 8 символов занимает
	 *         хэш-код исходного идентификатора.
	 */
	public static String limitName(String value) {
		String result = value;
		if (result.length() > NamedElement.MAX_IDENTIFIER_LENGTH) {
			result = String.format("%s%08X", result.substring(0, NamedElement.MAX_IDENTIFIER_LENGTH - 8),
					result.hashCode());
		}
		return result;
	}

	/**
	 * Возвращает имя.
	 */
	public final String getName() {
		return name;
	}

	/**
	 * Возвращает имя в прямых кавычках ("ANSI quotes").
	 */
	public final String getQuotedName() {
		return quotedName;
	}

	@Override
	public final int hashCode() {
		return name.hashCode();
	}

	@Override
	public final boolean equals(Object obj) {
		return obj instanceof NamedElement ? name.equals(((NamedElement) obj).getName()) : name.equals(obj);
	}

	/**
	 * Возвращает значение документационной строки для данного элемента.
	 */
	public String getCelestaDoc() {
		return celestaDoc;
	}

	/**
	 * Устанавливает значение документационной строки в закомментированном виде.
	 * 
	 * @param celestaDoc
	 *            новое значение.
	 * @throws ParseException
	 *             Если комментарий имеет неверный формат.
	 */
	void setCelestaDocLexem(String celestaDoc) throws ParseException {
		if (celestaDoc == null)
			this.celestaDoc = null;
		else {
			Matcher m = COMMENT.matcher(celestaDoc);
			if (!m.matches())
				throw new ParseException("Celestadoc should match pattern /**...*/, was " + celestaDoc);
			setCelestaDoc(m.group(1));
		}
	}

	/**
	 * Устанавливает значение документационной строки.
	 * 
	 * @param celestaDoc
	 *            новое значение.
	 * @throws ParseException
	 *            неверный CelestaDoc.
	 */
	public void setCelestaDoc(String celestaDoc) throws ParseException {
		this.celestaDoc = celestaDoc;
	}
}

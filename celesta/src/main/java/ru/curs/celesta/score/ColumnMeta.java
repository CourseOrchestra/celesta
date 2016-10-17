package ru.curs.celesta.score;

import ru.curs.celesta.CelestaException;

/**
 * Информация о типе столбца таблицы или представления .
 */
public interface ColumnMeta {

	/**
	 * Имя jdbcGetter-а, которое следует использовать для получения данных
	 * столбца.
	 */
	String jdbcGetterName();

	/**
	 * Тип данных Celesta,соответствующий полю.
	 */
	String getCelestaType();

	/**
	 * Является ли поле nullable.
	 */
	boolean isNullable();

	/**
	 * Column's CelestaDoc.
	 */
	String getCelestaDoc();

	/**
	 * Extracts first occurence of JSON object string from CelestaDoc.
	 * 
	 * @throws CelestaException
	 *             Broken or truncated JSON.
	 */
	// CHECKSTYLE:OFF for cyclomatic complexity
	default String getCelestaDocJSON() throws CelestaException {
		// CHECKSTYLE:ON
		String celestaDoc = getCelestaDoc();

		if (celestaDoc == null)
			return "{}";
		StringBuilder sb = new StringBuilder();
		int state = 0;
		int bracescount = 0;
		for (int i = 0; i < celestaDoc.length(); i++) {
			char c = celestaDoc.charAt(i);
			switch (state) {
			case 0:
				if (c == '{') {
					sb.append(c);
					bracescount++;
					state = 1;
				}
				break;
			case 1:
				sb.append(c);
				if (c == '{') {
					bracescount++;
				} else if (c == '}') {
					if (--bracescount == 0)
						return sb.toString();
				} else if (c == '"') {
					state = 2;
				}
				break;
			case 2:
				sb.append(c);
				if (c == '\\') {
					state = 3;
				} else if (c == '"') {
					state = 1;
				}
				break;
			case 3:
				sb.append(c);
				state = 2;
				break;
			default:
			}
		}
		// No valid json!
		if (state != 0)
			throw new CelestaException("Broken or truncated JSON: %s", sb.toString());
		return "{}";
	}

}

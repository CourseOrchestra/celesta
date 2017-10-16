package ru.curs.celesta.score;

import java.io.BufferedWriter;
import java.io.IOException;

/**
 * Булевская колонка (тип BIT).
 * 
 */
public final class BooleanColumn extends Column {
	/**
	 * Celesta-тип данных колонки.
	 */
	public static final String CELESTA_TYPE = "BIT";
	private Boolean defaultvalue;

	public BooleanColumn(TableElement table, String name) throws ParseException {
		super(table, name);
	}

	@Override
	protected void setDefault(String lexvalue) throws ParseException {
		defaultvalue = parseSQLBool(lexvalue);
	}

	/**
	 * Производит разбор строки в SQL-определении DEFAULT в boolean-значение.
	 * 
	 * @param lexvalue
	 *            строковое определение.
	 * 
	 * @throws ParseException
	 *             неверный формат строки.
	 */
	public static Boolean parseSQLBool(String lexvalue) throws ParseException {
		if (lexvalue == null) {
			return null;
		} else if ("'TRUE'".equalsIgnoreCase(lexvalue) || "TRUE".equalsIgnoreCase(lexvalue) || "1".equals(lexvalue)) {
			return true;
		} else if ("'FALSE'".equalsIgnoreCase(lexvalue) || "FALSE".equalsIgnoreCase(lexvalue) || "0".equals(lexvalue)) {
			return false;
		} else {
			throw new ParseException("Default boolean value should be either 'TRUE'/1 or 'FALSE'/0.");
		}
	}

	@Override
	public Boolean getDefaultValue() {
		return defaultvalue;
	}

	@Override
	public String jdbcGetterName() {
		return "getBoolean";
	}

	@Override
	void save(BufferedWriter bw) throws IOException {
		super.save(bw);
		bw.write(" BIT");
		if (!isNullable())
			bw.write(" NOT NULL");
		Boolean defaultVal = getDefaultValue();
		if (defaultVal != null) {
			bw.write(" DEFAULT '");
			bw.write(defaultVal.toString().toUpperCase());
			bw.write("'");
		}

	}

	@Override
	public String getCelestaType() {
		return CELESTA_TYPE;
	}

	@Override
	public String getCelestaDefault() {
		return defaultvalue == null ? null : ("'" + defaultvalue.toString().toUpperCase() + "'");
	}
}

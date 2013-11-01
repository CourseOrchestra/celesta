package ru.curs.celesta.score;

import java.io.BufferedWriter;
import java.io.IOException;

/**
 * Булевская колонка (тип BIT).
 * 
 */
public final class BooleanColumn extends Column {
	private Boolean defaultvalue;

	public BooleanColumn(Table table, String name) throws ParseException {
		super(table, name);
	}

	@Override
	protected void setDefault(String lexvalue) throws ParseException {
		if (lexvalue == null) {
			defaultvalue = null;
		} else if ("'TRUE'".equalsIgnoreCase(lexvalue) || "1".equals(lexvalue)) {
			defaultvalue = true;
		} else if ("'FALSE'".equalsIgnoreCase(lexvalue) || "0".equals(lexvalue)) {
			defaultvalue = false;
		} else {
			throw new ParseException(
					"Default boolean value should be either 'TRUE'/1 or 'FALSE'/1.");
		}
	}

	/**
	 * Возвращает значение по умолчанию.
	 */
	public Boolean getDefaultValue() {
		return defaultvalue;
	}

	@Override
	public String pythonDefaultValue() {
		return "False";
	}

	@Override
	public String jdbcGetterName() {
		return "getBoolean";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see ru.curs.celesta.score.Column#save(java.io.BufferedWriter)
	 */
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

}

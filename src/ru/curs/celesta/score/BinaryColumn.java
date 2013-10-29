package ru.curs.celesta.score;

import java.io.BufferedWriter;
import java.io.IOException;

/**
 * Бинарная колонка (тип IMAGE или BLOB).
 * 
 */
public final class BinaryColumn extends Column {

	private String defaultvalue;

	public BinaryColumn(Table table, String name) throws ParseException {
		super(table, name);
	}

	@Override
	protected void setDefault(String lexvalue) {
		defaultvalue = lexvalue;
	}

	/**
	 * Возвращает значение по умолчанию.
	 */
	public String getDefaultValue() {
		return defaultvalue;
	}

	@Override
	protected String getDefaultDefault() {
		return "0x00";
	}

	@Override
	public String pythonDefaultValue() {
		return "None";
	}

	@Override
	public String jdbcGetterName() {
		return "getBlob";
	}

	@Override
	void save(BufferedWriter bw) throws IOException {
		super.save(bw);
		bw.write(" IMAGE");
		if (!isNullable())
			bw.write(" NOT NULL");
		String defaultVal = getDefaultValue();
		if (defaultVal != null) {
			bw.write(" DEFAULT ");
			bw.write(defaultVal);
		}
	}
}

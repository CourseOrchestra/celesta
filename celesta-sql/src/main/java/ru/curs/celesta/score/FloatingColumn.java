package ru.curs.celesta.score;

import java.io.IOException;
import java.io.PrintWriter;

/**
 * Колонка с типом REAL.
 * 
 */
public final class FloatingColumn extends Column {
	/**
	 * Celesta-тип данных колонки.
	 */
	public static final String CELESTA_TYPE = "REAL";

	private Double defaultValue;

	public FloatingColumn(TableElement table, String name) throws ParseException {
		super(table, name);
	}

	@Override
	protected void setDefault(String lexvalue) {
		defaultValue = (lexvalue == null) ? null : Double.parseDouble(lexvalue);
	}

	@Override
	public Double getDefaultValue() {
		return defaultValue;
	}

	@Override
	public String jdbcGetterName() {
		return "getDouble";
	}

	@Override
	void save(PrintWriter bw) throws IOException {
		super.save(bw);
		bw.write(" REAL");
		if (!isNullable())
			bw.write(" NOT NULL");
		Double defaultVal = getDefaultValue();
		if (defaultVal != null) {
			bw.write(" DEFAULT ");
			bw.write(defaultVal.toString());
		}
	}

	@Override
	public String getCelestaType() {
		return CELESTA_TYPE;
	}

	@Override
	public Class getJavaClass() {
		return Double.class;
	}

	@Override
	public String getCelestaDefault() {
		return defaultValue == null ? null : defaultValue.toString();
	}

}

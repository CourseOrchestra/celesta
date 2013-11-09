package ru.curs.celesta.score;

import java.io.BufferedWriter;
import java.io.IOException;

/**
 * Колонка с типом REAL.
 * 
 */
public final class FloatingColumn extends Column {

	private Double defaultvalue;

	public FloatingColumn(Table table, String name) throws ParseException {
		super(table, name);
	}

	@Override
	protected void setDefault(String lexvalue) {
		defaultvalue = (lexvalue == null) ? null : Double.parseDouble(lexvalue);
	}

	@Override
	public Double getDefaultValue() {
		return defaultvalue;
	}

	@Override
	public String pythonDefaultValue() {
		return "0";
	}

	@Override
	public String jdbcGetterName() {
		return "getDouble";
	}

	@Override
	void save(BufferedWriter bw) throws IOException {
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

}

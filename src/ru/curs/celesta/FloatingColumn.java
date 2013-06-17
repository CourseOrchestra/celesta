package ru.curs.celesta;

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

	public Double getDefaultvalue() {
		return defaultvalue;
	}
}

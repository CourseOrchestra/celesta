package ru.curs.celesta;

public final class FloatingColumn extends Column {
	public FloatingColumn(String name) {
		super(name);
	}

	private Double defaultvalue;

	@Override
	protected void setDefault(String lexvalue) {
		defaultvalue = (lexvalue == null) ? null : Double.parseDouble(lexvalue);
	}

	public Double getDefaultvalue() {
		return defaultvalue;
	}
}

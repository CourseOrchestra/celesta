package ru.curs.celesta;

public class IntegerColumn extends Column {
	public IntegerColumn(String name) {
		super(name);
	}

	private Integer defaultvalue;

	@Override
	protected void setDefault(String lexvalue) {
		defaultvalue = (lexvalue == null) ? null : Integer.parseInt(lexvalue);
	}

	public Integer getDefaultvalue() {
		return defaultvalue;
	}
}

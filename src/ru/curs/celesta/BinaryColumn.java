package ru.curs.celesta;

public final class BinaryColumn extends Column {

	private String defaultvalue;

	public BinaryColumn(String name) {
		super(name);
	}

	@Override
	protected void setDefault(String lexvalue) {
		defaultvalue = lexvalue;
	}

	public Object getDefaultValue() {
		return defaultvalue;
	}

}

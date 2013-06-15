package ru.curs.celesta;

public final class BinaryColumn extends Column {

	private String defaultvalue;

	public BinaryColumn(Table table, String name) throws ParseException {
		super(table, name);
	}

	@Override
	protected void setDefault(String lexvalue) {
		defaultvalue = lexvalue;
	}

	public Object getDefaultValue() {
		return defaultvalue;
	}

}

package ru.curs.celesta;

public final class BooleanColumn extends Column {
	public BooleanColumn(String name) {
		super(name);
	}

	private Boolean defaultvalue;

	@Override
	protected void setDefault(String lexvalue) throws ParseException {
		if (lexvalue == null)
			defaultvalue = null;
		else if ("'TRUE'".equalsIgnoreCase(lexvalue))
			defaultvalue = true;
		else if ("'FALSE'".equalsIgnoreCase(lexvalue))
			defaultvalue = false;
		else
			throw new ParseException(
					"Default boolean value should be either 'TRUE' or 'FALSE'");
	}

	public Boolean getDefaultvalue() {
		return defaultvalue;
	}

}

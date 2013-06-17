package ru.curs.celesta;

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
		} else if ("'TRUE'".equalsIgnoreCase(lexvalue)) {
			defaultvalue = true;
		} else if ("'FALSE'".equalsIgnoreCase(lexvalue)) {
			defaultvalue = false;
		} else {
			throw new ParseException(
					"Default boolean value should be either 'TRUE' or 'FALSE'");
		}
	}

	public Boolean getDefaultvalue() {
		return defaultvalue;
	}

}

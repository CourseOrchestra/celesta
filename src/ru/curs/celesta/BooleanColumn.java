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
		} else if ("'TRUE'".equalsIgnoreCase(lexvalue) || "1".equals(lexvalue)) {
			defaultvalue = true;
		} else if ("'FALSE'".equalsIgnoreCase(lexvalue) || "0".equals(lexvalue)) {
			defaultvalue = false;
		} else {
			throw new ParseException(
					"Default boolean value should be either 'TRUE'/1 or 'FALSE'/1.");
		}
	}

	/**
	 * Возвращает значение по умолчанию.
	 */
	public Boolean getDefaultvalue() {
		return defaultvalue;
	}

	@Override
	protected String getDefaultDefault() {
		return "0";
	}

}

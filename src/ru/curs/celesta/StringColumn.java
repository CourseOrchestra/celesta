package ru.curs.celesta;

/**
 * Строковая колонка.
 * 
 */
public final class StringColumn extends Column {

	private static final String INVALID_QUOTED_FORMAT = "Invalid quoted string format.";

	private String defaultvalue;
	private int length;
	private boolean max;

	public StringColumn(Table table, String name) throws ParseException {
		super(table, name);
	}

	@Override
	protected void setDefault(String lexvalue) throws ParseException {
		if (lexvalue == null) {
			defaultvalue = null;
			return;
		}
		defaultvalue = unquoteString(lexvalue);
	}

	public static String unquoteString(String lexvalue) throws ParseException {
		StringBuilder sb = new StringBuilder();
		int state = 0;
		for (int i = 0; i < lexvalue.length(); i++) {
			char c = lexvalue.charAt(i);
			switch (state) {
			case 0:
				if (c == '\'') {
					state = 1;
				} else {
					throw new ParseException(INVALID_QUOTED_FORMAT);
				}
				break;
			case 1:
				if (c == '\'') {
					state = 2;
				} else {
					sb.append(c);
				}
				break;
			case 2:
				if (c == '\'') {
					sb.append('\'');
					state = 1;
				} else {
					throw new ParseException(INVALID_QUOTED_FORMAT);
				}
			default:
			}
		}
		return sb.toString();
	}

	/**
	 * Значение по умолчанию.
	 */
	public String getDefaultValue() {
		return defaultvalue;
	}

	/**
	 * Максимальная длина текстового поля. Не должна учитываться, если
	 * isMax()==true.
	 */
	public int getLength() {
		return length;
	}

	/**
	 * Указывает на то, что при определении поля вместо длины был передан
	 * параметр MAX.
	 */
	public boolean isMax() {
		return max;
	}

	void setLength(String length) {
		if ("MAX".equalsIgnoreCase(length)) {
			max = true;
			this.length = 0;
		} else {
			max = false;
			this.length = Integer.parseInt(length);
		}
	}

}

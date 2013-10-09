package ru.curs.celesta.score;

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

	/**
	 * Расковычивает строки в закавыченном формате.
	 * 
	 * @param lexvalue
	 *            закавыченная строка.
	 * @throws ParseException
	 *             неверный формат.
	 */
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
	 * Закавычевает строки.
	 * 
	 * @param lexvalue
	 *            строка для закавычивания.
	 */
	public static String quoteString(String lexvalue) {
		StringBuilder sb = new StringBuilder();
		sb.append('\'');
		for (int i = 0; i < lexvalue.length(); i++) {
			char c = lexvalue.charAt(i);
			sb.append(c);
			if (c == '\'')
				sb.append('\'');
		}
		sb.append('\'');
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

	/**
	 * Устанавливает длину текстового поля.
	 * 
	 * @param length
	 *            Новая длина
	 * @throws ParseException
	 *             Если указана нулевая или отрицательная длина.
	 */
	public void setLength(String length) throws ParseException {
		if ("MAX".equalsIgnoreCase(length)) {
			getParentTable().getGrain().modify();
			max = true;
			this.length = 0;
		} else {
			max = false;
			int newLength = Integer.parseInt(length);
			if (newLength <= 0)
				throw new ParseException(
						String.format(
								"String column length for column '%s' must be greater than zero.",
								getName()));
			getParentTable().getGrain().modify();
			this.length = newLength;
		}
	}

	@Override
	protected String getDefaultDefault() {
		return "''";
	}

	@Override
	public String pythonDefaultValue() {
		return "''";
	}

	@Override
	public String jdbcGetterName() {
		return "getString";
	}
}

package ru.curs.celesta.score;

import java.io.BufferedWriter;
import java.io.IOException;

/**
 * Строковая колонка.
 * 
 */
public final class StringColumn extends Column {
	/**
	 * Celesta-тип данных колонки для короткой строки.
	 */
	public static final String VARCHAR = "VARCHAR";
	/**
	 * Celesta-тип данных колонки для длинной строки.
	 */
	public static final String TEXT = "TEXT";

	private static final String INVALID_QUOTED_FORMAT = "Invalid quoted string format.";

	private String defaultvalue;
	private int length;
	private boolean max;

	public StringColumn(TableElement table, String name) throws ParseException {
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

	@Override
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
			int newLength;
			try {
				newLength = Integer.parseInt(length);
			} catch (NumberFormatException e) {
				throw new ParseException(
						String.format("Invalid string column length '%s' for column '%s' of table '%s'", length,
								getName(), getParentTable().getName()));
			}
			if (newLength <= 0)
				throw new ParseException(
						String.format("String column length for column '%s' must be greater than zero.", getName()));
			getParentTable().getGrain().modify();
			this.length = newLength;
		}
	}

	@Override
	public String jdbcGetterName() {
		return "getString";
	}

	@Override
	void save(BufferedWriter bw) throws IOException {
		super.save(bw);
		if (isMax())
			bw.write(" TEXT");
		else {
			bw.write(" VARCHAR(");
			bw.write(Integer.toString(getLength()));
			bw.write(")");
		}

		if (!isNullable())
			bw.write(" NOT NULL");
		String defaultVal = getDefaultValue();
		if (defaultVal != null) {
			bw.write(" DEFAULT ");
			bw.write(quoteString(defaultVal));
		}
	}

	@Override
	public String getCelestaType() {
		return max ? TEXT : VARCHAR;
	}

	@Override
	public String getCelestaDefault() {
		return defaultvalue == null ? null : quoteString(defaultvalue);
	}
}

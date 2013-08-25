package ru.curs.celesta.score;

import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Колонка дат (DATETIME).
 * 
 */
public final class DateTimeColumn extends Column {

	private static final Pattern P = Pattern
			.compile("'(\\d\\d\\d\\d)([01]\\d)([0123]\\d)'");

	private Date defaultvalue;

	private boolean getdate;

	public DateTimeColumn(Table table, String name) throws ParseException {
		super(table, name);
	}

	@Override
	protected void setDefault(String lexvalue) throws ParseException {

		if (lexvalue == null) {
			defaultvalue = null;
			getdate = false;

		} else if ("GETDATE".equalsIgnoreCase(lexvalue)) {
			defaultvalue = null;
			getdate = true;
		} else {
			defaultvalue = parseISODate(lexvalue);
			getdate = false;
		}
	}

	/**
	 * Выполняет разбор даты в формате YYYYMMDD и преобразование в Java-объект
	 * Date.
	 * 
	 * @param lexvalue
	 *            текстовое значение.
	 * @throws ParseException
	 *             В случае, если текстовое значение не соответствует паттерну
	 *             YYYYMMDD.
	 */
	public static Date parseISODate(String lexvalue) throws ParseException {
		Matcher m = P.matcher(lexvalue);
		if (!m.matches())
			throw new ParseException(
					String.format(
							"Invalid default datetime value %s. It should match 'YYYYMMDD' pattern.",
							lexvalue));
		int y = Integer.parseInt(m.group(1));
		int mo = Integer.parseInt(m.group(2));
		int d = Integer.parseInt(m.group(3));

		Calendar c = Calendar.getInstance();
		c.clear();

		c.set(y, mo - 1, d);
		return c.getTime();
	}

	/**
	 * Значение по умолчанию.
	 */
	public Date getDefaultValue() {
		return defaultvalue;
	}

	/**
	 * Используется ли конструкция GETDATE() в качестве значения по умолчанию.
	 */
	public boolean isGetdate() {
		return getdate;
	}

	@Override
	protected String getDefaultDefault() {
		return "GETDATE";
	}

	@Override
	public String pythonDefaultValue() {
		return "None";
	}

	@Override
	public String jdbcGetterName() {
		return "getDate";
	}

}

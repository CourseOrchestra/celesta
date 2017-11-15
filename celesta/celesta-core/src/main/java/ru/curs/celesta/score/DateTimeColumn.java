package ru.curs.celesta.score;

import java.io.BufferedWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Колонка дат (DATETIME).
 * 
 */
public final class DateTimeColumn extends Column {

	/**
	 * Celesta-тип данных колонки.
	 */
	public static final String CELESTA_TYPE = "DATETIME";

	private static final Pattern P = Pattern
			.compile("'(\\d\\d\\d\\d)([01]\\d)([0123]\\d)'");

	private Date defaultvalue;

	private boolean getdate;

	public DateTimeColumn(TableElement table, String name) throws ParseException {
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
							"Invalid datetime value %s. It should match 'YYYYMMDD' pattern.",
							lexvalue));
		int y = Integer.parseInt(m.group(1));
		int mo = Integer.parseInt(m.group(2));
		int d = Integer.parseInt(m.group(3));

		Calendar c = Calendar.getInstance();
		c.clear();

		c.set(y, mo - 1, d);
		return c.getTime();
	}

	@Override
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
	public String jdbcGetterName() {
		return "getTimestamp";
	}

	@Override
	void save(BufferedWriter bw) throws IOException {
		super.save(bw);
		bw.write(" DATETIME");
		if (!isNullable())
			bw.write(" NOT NULL");
		if (isGetdate())
			bw.write(" DEFAULT GETDATE()");
		else {
			Date defaultVal = getDefaultValue();
			if (defaultVal != null) {
				bw.write(" DEFAULT '");
				DateFormat df = new SimpleDateFormat("yyyyMMdd");
				bw.write(df.format(defaultVal));
				bw.write("'");
			}
		}
	}

	@Override
	public String getCelestaType() {
		return CELESTA_TYPE;
	}

	@Override
	public String getCelestaDefault() {
		if (isGetdate()) {
			return "GETDATE()";
		} else {
			if (defaultvalue == null) {
				return null;
			} else {
				DateFormat df = new SimpleDateFormat("yyyyMMdd");
				return "'" + df.format(defaultvalue) + "'";
			}
		}
	}

}

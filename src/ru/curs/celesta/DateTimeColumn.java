package ru.curs.celesta;

import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateTimeColumn extends Column {

	private static final Pattern p = Pattern
			.compile("'(\\d\\d\\d\\d)(\\d\\d)(\\d\\d)'");

	public DateTimeColumn(String name) {
		super(name);
	}

	private Date defaultvalue;

	@Override
	protected void setDefault(String lexvalue) throws ParseException {

		if (lexvalue == null) {
			defaultvalue = null;
			return;
		}
		Matcher m = p.matcher(lexvalue);
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
		defaultvalue = c.getTime();
	}

	public Date getDefaultValue() {
		return defaultvalue;
	}

}

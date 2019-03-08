package ru.curs.celesta.score;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Date column (DATETIME).
 */
public final class DateTimeColumn extends Column {

    /**
     * Celesta type of the column data.
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
     * Parses the date in YYYYMMDD format and translates it to Java object.
     *
     * @param lexvalue  text value.
     * @return
     * @throws ParseException  in case if the text value doesn't correspond to 
     *                         YYYYMMDD pattern.
     */
    public static Date parseISODate(String lexvalue) throws ParseException {
        Matcher m = P.matcher(lexvalue);
        if (!m.matches()) {
            throw new ParseException(
                    String.format(
                            "Invalid datetime value %s. It should match 'YYYYMMDD' pattern.",
                            lexvalue));
        }
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
     * Whether the construction GETDATE() is used as a default value.
     *
     * @return
     */
    public boolean isGetdate() {
        return getdate;
    }

    @Override
    public String jdbcGetterName() {
        return "getTimestamp";
    }

    @Override
    public String getCelestaType() {
        return CELESTA_TYPE;
    }

    @Override
    public Class<?> getJavaClass() {
        return Date.class;
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

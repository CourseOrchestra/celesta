package ru.curs.celesta.score;

import java.time.ZonedDateTime;

/**
 * Column for date with time zone type.
 */
public final class ZonedDateTimeColumn extends Column<ZonedDateTime> {
    /**
     * Celesta type of the column data.
     */
    public static final String CELESTA_TYPE = "DATETIME WITH TIME ZONE";


    public ZonedDateTimeColumn(TableElement table, String name) throws ParseException {
        super(table, name);
    }

    @Override
    protected void setDefault(String lexvalue) throws ParseException {

        if (lexvalue != null) {
            throw new ParseException("Default values for type \"datetime with time zone\" isn't supported");
        }
    }

    @Override
    public ZonedDateTime getDefaultValue() {
        return null;
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
        return ZonedDateTime.class;
    }

    @Override
    public String getCelestaDefault() {
        return "";
    }

}

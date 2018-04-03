package ru.curs.celesta.score;

import java.io.IOException;
import java.io.PrintWriter;
import java.time.ZonedDateTime;
import java.util.Date;

public final class ZonedDateTimeColumn extends Column {
    /**
     * Celesta-тип данных колонки.
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
    void save(PrintWriter bw) throws IOException {
        super.save(bw);
        bw.write(" " + CELESTA_TYPE);
        if (!isNullable())
            bw.write(" NOT NULL");

    }

    @Override
    public String getCelestaType() {
        return CELESTA_TYPE;
    }

    @Override
    public Class getJavaClass() {
        return ZonedDateTime.class;
    }

    @Override
    public String getCelestaDefault() {
        return "";
    }
}

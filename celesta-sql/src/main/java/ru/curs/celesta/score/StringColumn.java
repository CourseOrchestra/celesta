package ru.curs.celesta.score;

import java.io.IOException;
import java.io.PrintWriter;

/**
 * String column.
 */
public final class StringColumn extends Column {
    /**
     * Celesta type of the column data for short string.
     */
    public static final String VARCHAR = "VARCHAR";
    /**
     * Celesta type of the column data for long string.
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
     * Unquotes a string that is quoted.
     *
     * @param lexvalue  quoted string
     * @throws ParseException  incorrect format
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
     * Quotes a string.
     *
     * @param lexvalue  string for quoting
     */
    public static String quoteString(String lexvalue) {
        StringBuilder sb = new StringBuilder();
        sb.append('\'');
        for (int i = 0; i < lexvalue.length(); i++) {
            char c = lexvalue.charAt(i);
            sb.append(c);
            if (c == '\'') {
                sb.append('\'');
            }
        }
        sb.append('\'');
        return sb.toString();
    }

    @Override
    public String getDefaultValue() {
        return defaultvalue;
    }

    /**
     * Maximal length of the text field. It should not be taken into account if
     * <code>isMax()==true</code>.
     *
     * @return
     */
    public int getLength() {
        return length;
    }

    /**
     * Indicates that on the field definition MAX parameter was provided
     * instead of the length.
     *
     * @return
     */
    public boolean isMax() {
        return max;
    }

    /**
     * Sets length of the text field.
     *
     * @param length  new length
     * @throws ParseException  if zero or negative length is specified.
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
            if (newLength <= 0) {
                throw new ParseException(
                        String.format("String column length for column '%s' must be greater than zero.", getName()));
            }
            getParentTable().getGrain().modify();
            this.length = newLength;
        }
    }

    @Override
    public String jdbcGetterName() {
        return "getString";
    }

    @Override
    void save(PrintWriter bw) throws IOException {
        super.save(bw);
        if (isMax()) {
            bw.write(" TEXT");
        } else {
            bw.write(" VARCHAR(");
            bw.write(Integer.toString(getLength()));
            bw.write(")");
        }

        if (!isNullable()) {
            bw.write(" NOT NULL");
        }
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
    public Class<?> getJavaClass() {
        return String.class;
    }

    @Override
    public String getCelestaDefault() {
        return defaultvalue == null ? null : quoteString(defaultvalue);
    }

}

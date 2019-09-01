package ru.curs.celesta.score;

/**
 * Boolean column (BIT type).
 */
public final class BooleanColumn extends Column<Boolean> {
    /**
     * Celesta type of the column data.
     */
    public static final String CELESTA_TYPE = "BIT";
    private Boolean defaultvalue;

    public BooleanColumn(TableElement table, String name) throws ParseException {
        super(table, name);
    }

    @Override
    protected void setDefault(String lexvalue) throws ParseException {
        defaultvalue = parseSQLBool(lexvalue);
    }

    /**
     * Parses a string in SQL definition DEFAULT to a boolean value.
     *
     * @param lexvalue  string definition
     * @return
     *
     * @throws ParseException  incorrect string format
     */
    public static Boolean parseSQLBool(String lexvalue) throws ParseException {
        if (lexvalue == null) {
            return null;
        } else if ("'TRUE'".equalsIgnoreCase(lexvalue) || "TRUE".equalsIgnoreCase(lexvalue) || "1".equals(lexvalue)) {
            return true;
        } else if ("'FALSE'".equalsIgnoreCase(lexvalue) || "FALSE".equalsIgnoreCase(lexvalue) || "0".equals(lexvalue)) {
            return false;
        } else {
            throw new ParseException("Default boolean value should be either 'TRUE'/1 or 'FALSE'/0.");
        }
    }

    @Override
    public Boolean getDefaultValue() {
        return defaultvalue;
    }

    @Override
    public String jdbcGetterName() {
        return "getBoolean";
    }

    @Override
    public String getCelestaType() {
        return CELESTA_TYPE;
    }

    @Override
    public Class<?> getJavaClass() {
        return Boolean.class;
    }

    @Override
    public String getCelestaDefault() {
        return defaultvalue == null ? null : ("'" + defaultvalue.toString().toUpperCase() + "'");
    }

}

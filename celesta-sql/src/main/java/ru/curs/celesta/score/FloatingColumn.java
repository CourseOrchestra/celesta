package ru.curs.celesta.score;

/**
 * Column REAL type.
 */
public final class FloatingColumn extends Column<Double> {
    /**
     * Celesta type of the column data.
     */
    public static final String CELESTA_TYPE = "REAL";

    private Double defaultValue;

    public FloatingColumn(TableElement table, String name) throws ParseException {
        super(table, name);
    }

    @Override
    protected void setDefault(String lexvalue) {
        defaultValue = (lexvalue == null) ? null : Double.parseDouble(lexvalue);
    }

    @Override
    public Double getDefaultValue() {
        return defaultValue;
    }

    @Override
    public String jdbcGetterName() {
        return "getDouble";
    }

    @Override
    public String getCelestaType() {
        return CELESTA_TYPE;
    }

    @Override
    public Class<?> getJavaClass() {
        return Double.class;
    }

    @Override
    public String getCelestaDefault() {
        return defaultValue == null ? null : defaultValue.toString();
    }

}

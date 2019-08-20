package ru.curs.celesta.score;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Column with a fixed decimal point type.
 */
public final class DecimalColumn extends Column<BigDecimal> {

    //driven by MsSql
    public static final int MAX_PRECISION = 38;

    /**
     * Celesta type of the column data.
     */
    public static final String CELESTA_TYPE = "DECIMAL";

    private int precision;
    private int scale;

    private BigDecimal defaultValue;

    public DecimalColumn(TableElement table, String name, int precision, int scale) throws ParseException {
        super(table, name);

        if (precision < 1 || precision > MAX_PRECISION) {
            throw new ParseException(
                    String.format(
                            "Illegal precision %s (must be between 1 and %s inclusive) for column %s.%s.%s",
                            precision, MAX_PRECISION, getParentTable().getGrain().getName(),
                            getParentTable().getName(), getName())
            );
        }

        if (scale < 0 || scale > precision) {
            throw new ParseException(
                    String.format(
                            "Illegal scale %s (must be between 0 and %s inclusive) for column %s.%s.%s",
                            scale, precision, getParentTable().getGrain().getName(),
                            getParentTable().getName(), getName())
            );
        }

        this.precision = precision;
        this.scale = scale;
    }

    @Override
    protected void setDefault(String lexValue) throws ParseException {
        BigDecimal value = (lexValue == null) ? null : new BigDecimal(lexValue);

        if (value != null) {
            int wholePartMaxLength = this.precision - this.scale;

            BigInteger wholePart = value.toBigInteger();
            int wholePartLength = wholePart.toString().length();

            if (wholePartLength > wholePartMaxLength) {
                throw new ParseException(
                        String.format(
                                "Illegal default value %s for column %s.%s.%s is bigger than specified precision",
                                value.toPlainString(), getParentTable().getGrain().getName(),
                                getParentTable().getName(), getName())
                );
            }
        }

        this.defaultValue = (value == null) ? null : value;
    }

    @Override
    public BigDecimal getDefaultValue() {
        return this.defaultValue;
    }

    @Override
    public String getCelestaDefault() {
        return defaultValue == null ? null : defaultValue.toString();
    }

    @Override
    public String jdbcGetterName() {
        return "getBigDecimal";
    }

    @Override
    public String getCelestaType() {
        return CELESTA_TYPE;
    }

    @Override
    public Class<?> getJavaClass() {
        return BigDecimal.class;
    }

    /**
     * Returns precision.
     *
     * @return
     */
    public int getPrecision() {
        return precision;
    }

    /**
     * Sets precision.
     *
     * @param precision  precision
     */
    public void setPrecision(int precision) {
        this.precision = precision;
    }

    /**
     * Returns scale.
     *
     * @return
     */
    public int getScale() {
        return scale;
    }

    /**
     * Sets scale.
     *
     * @param scale  scale
     */
    public void setScale(int scale) {
        this.scale = scale;
    }

}

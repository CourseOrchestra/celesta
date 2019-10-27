package ru.curs.celesta.dbutils.meta;

import ru.curs.celesta.score.*;

import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Column data in the database in form that is needed for Celesta.
 */
public final class DbColumnInfo {
    public static final String SEQUENCE_NEXT_VAL_PATTERN = "(?i)NEXTVAL\\((.*)\\)";

    private String name;
    private Class<? extends Column<?>> type;
    private boolean isNullable;
    private String defaultValue = "";
    private int length;
    private int scale;
    private boolean isMax;

    /**
     * Returns column name.
     *
     * @return
     */
    public String getName() {
        return name;
    }

    /**
     * Returns column type.
     *
     * @return
     */
    public Class<? extends Column<?>> getType() {
        return type;
    }

    /**
     * Whether column is nullable.
     *
     * @return
     */
    public boolean isNullable() {
        return isNullable;
    }

    /**
     * Column default value.
     *
     * @return
     */
    public String getDefaultValue() {
        return defaultValue;
    }

    /**
     * Column length.
     *
     * @return
     */
    public int getLength() {
        return length;
    }

    public boolean isMax() {
        return isMax;
    }

    /**
     * Sets column name.
     *
     * @param name  column name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Sets column type.
     *
     * @param type  column type
     */
    public void setType(Class<? extends Column<?>> type) {
        this.type = type;
    }

    /**
     * Sets if column is nullable.
     *
     * @param isNullable  {@code true} if column is nullable otherwise {@code false}
     */
    public void setNullable(boolean isNullable) {
        this.isNullable = isNullable;
    }

    /**
     * Sets column default value.
     *
     * @param defaultValue  column default value
     */
    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    /**
     * Sets column length.
     *
     * @param length  column length
     */
    public void setLength(int length) {
        this.length = length;
    }

    public int getScale() {
        return scale;
    }

    public void setScale(int scale) {
        this.scale = scale;
    }

    public void setMax(boolean isMax) {
        this.isMax = isMax;
    }

    public boolean reflects(Column<?> value) {
        // If the type doesn't match -- don't check further.
        if (value.getClass() != type) {
            return false;
        }

        // Checking for nullability with keeping in mind that in Oracle DEFAULT
        // ''-strings are always nullable
        if (type != StringColumn.class || !"''".equals(defaultValue)) {
            if (value.isNullable() != isNullable) {
                return false;
            }
        }

        if (type == StringColumn.class) {
            // If length parameters do not match -- don't check
            StringColumn col = (StringColumn) value;
            if (!(isMax ? col.isMax() : length == col.getLength())) {
                return false;
            }
        }

        if (this.type == DecimalColumn.class) {
            DecimalColumn dc = (DecimalColumn) value;
            if (dc.getPrecision() != this.length || dc.getScale() != this.scale) {
                return false;
            }
        }

        // If there's an empty default in data, and non-empty one in metadata -- don't check
        if (defaultValue.isEmpty()) {
            if (type == DateTimeColumn.class) {
                //do not forget DateTime's special case
                DateTimeColumn dtc = (DateTimeColumn) value;
                return dtc.getDefaultValue() == null && (!dtc.isGetdate());
            } else if (type == IntegerColumn.class) {
                IntegerColumn ic = (IntegerColumn) value;
                return ic.getDefaultValue() == null && ic.getSequence() == null;
            } else {
                return value.getDefaultValue() == null;
            }
        }
        // A case of a non-empty default-value in data.
        return checkDefault(value);
    }

    private boolean checkDefault(Column<?> value) {
        boolean result;
        if (type == BooleanColumn.class) {
            try {
                result = BooleanColumn.parseSQLBool(defaultValue).equals(value.getDefaultValue());
            } catch (ParseException e) {
                result = false;
            }
        } else if (type == IntegerColumn.class) {
            Pattern p = Pattern.compile(SEQUENCE_NEXT_VAL_PATTERN);
            IntegerColumn iValue = (IntegerColumn) value;
            if (iValue.getSequence() != null) {
                Matcher m = p.matcher(defaultValue);
                result = m.matches();
                if (result) {
                    String sequenceName = m.group(1);
                    result = sequenceName.equals(iValue.getSequence().getName());
                }
            } else if (!p.matcher(defaultValue).matches()) {
                result = Integer.valueOf(defaultValue).equals(value.getDefaultValue());
            } else {
                result = false;
            }
        } else if (type == FloatingColumn.class) {
            result = Double.valueOf(defaultValue).equals(value.getDefaultValue());
        } else if (type == DecimalColumn.class) {
            DecimalColumn dc = (DecimalColumn) value;
            result = new BigDecimal(defaultValue).equals(dc.getDefaultValue());
        } else if (type == DateTimeColumn.class) {
            if ("GETDATE()".equalsIgnoreCase(defaultValue)) {
                result = ((DateTimeColumn) value).isGetdate();
            } else {
                try {
                    result = DateTimeColumn.parseISODate(defaultValue).equals(value.getDefaultValue());
                } catch (ParseException e) {
                    result = false;
                }
            }
        } else if (type == StringColumn.class) {
            result = defaultValue.equals(StringColumn.quoteString(((StringColumn) value).getDefaultValue()));
        } else {
            result = defaultValue.equals(value.getDefaultValue());
        }
        return result;
    }

}

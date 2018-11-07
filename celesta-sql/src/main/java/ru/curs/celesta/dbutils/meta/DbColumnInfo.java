package ru.curs.celesta.dbutils.meta;

import ru.curs.celesta.score.*;

import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Данные о колонке в базе данных в виде, необходимом для Celesta.
 */
public final class DbColumnInfo {
    private String name;
    private Class<? extends Column> type;
    private boolean isNullable;
    private String defaultValue = "";
    private int length;
    private int scale;
    private boolean isMax;

    public String getName() {
        return name;
    }

    public Class<? extends Column> getType() {
        return type;
    }

    public boolean isNullable() {
        return isNullable;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public int getLength() {
        return length;
    }

    public boolean isMax() {
        return isMax;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setType(Class<? extends Column> type) {
        this.type = type;
    }

    public void setNullable(boolean isNullable) {
        this.isNullable = isNullable;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

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

    public boolean reflects(Column value) {
        // Если тип не совпадает -- дальше не проверяем.
        if (value.getClass() != type)
            return false;

        // Проверяем nullability, но помним о том, что в Oracle DEFAULT
        // ''-строки всегда nullable
        if (type != StringColumn.class || !"''".equals(defaultValue)) {
            if (value.isNullable() != isNullable)
                return false;
        }

        if (type == StringColumn.class) {
            // Если параметры длин не совпали -- не проверяем
            StringColumn col = (StringColumn) value;
            if (!(isMax ? col.isMax() : length == col.getLength()))
                return false;
        }

        if (this.type == DecimalColumn.class) {
            DecimalColumn dc = (DecimalColumn)value;
            if (dc.getPrecision() != this.length || dc.getScale() != this.scale)
                return false;
        }

        // Если в данных пустой default, а в метаданных -- не пустой -- то
        // не проверяем
        if (defaultValue.isEmpty()) {
            if (type == DateTimeColumn.class) {
                //do not forget DateTime's special case
                DateTimeColumn dtc = (DateTimeColumn) value;
                return dtc.getDefaultValue() == null && dtc.isGetdate() == false;
            } else if (type == IntegerColumn.class) {
                IntegerColumn ic = (IntegerColumn) value;
                return ic.getDefaultValue() == null && ic.getSequence() == null;
            } else {
                return value.getDefaultValue() == null;
            }
        }
        // Случай непустого default-значения в данных.
        return checkDefault(value);
    }

    private boolean checkDefault(Column value) {
        boolean result;
        if (type == BooleanColumn.class) {
            try {
                result = BooleanColumn.parseSQLBool(defaultValue).equals(value.getDefaultValue());
            } catch (ParseException e) {
                result = false;
            }
        } else if (type == IntegerColumn.class) {
            Pattern p = Pattern.compile("(?i)NEXTVAL\\((.*)\\)");
            IntegerColumn iValue = (IntegerColumn)value;
            if (iValue.getSequence() != null) {
                Matcher m = p.matcher(defaultValue);
                if (result = m.matches()) {
                    String sequenceName = m.group(1);
                    result = sequenceName.equals(iValue.getSequence().getName());
                }
            }
            else if (!p.matcher(defaultValue).matches())
                result = Integer.valueOf(defaultValue).equals(value.getDefaultValue());
            else
                result = false;
        } else if (type == FloatingColumn.class) {
            result = Double.valueOf(defaultValue).equals(value.getDefaultValue());
        } else if (type == DecimalColumn.class) {
            DecimalColumn dc = (DecimalColumn)value;
            result = new BigDecimal(defaultValue).equals(dc.getDefaultValue());
        } else if (type == DateTimeColumn.class) {
            if ("GETDATE()".equalsIgnoreCase(defaultValue))
                result = ((DateTimeColumn) value).isGetdate();
            else {
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

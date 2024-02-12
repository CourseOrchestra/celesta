package ru.curs.celesta.dbutils.filter;


import java.util.Objects;

/**
 * Single value filter.
 */
public final class SingleValue extends AbstractFilter {
    private Object value;

    public SingleValue(Object value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return String.format("%s", value);
    }

    /**
     * Returns filter value.
     * @return
     */
    public Object getValue() {
        return value;
    }

    /**
     * Sets filter value.
     * @param value
     */
    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public boolean filterEquals(AbstractFilter f) {
        if (f instanceof SingleValue) {
            Object v2 = ((SingleValue) f).value;
            return Objects.equals(value, v2);
        } else {
            return false;
        }
    }
}

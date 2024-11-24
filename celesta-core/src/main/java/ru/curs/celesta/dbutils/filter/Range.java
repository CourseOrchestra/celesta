package ru.curs.celesta.dbutils.filter;


import java.util.Objects;

/**
 * Filter as a range of values from..to.
 */
public final class Range extends AbstractFilter {
    private Object valueFrom;
    private Object valueTo;

    public Range(Object valueFrom, Object valueTo) {
        this.valueFrom = valueFrom;
        this.valueTo = valueTo;
    }

    @Override
    public String toString() {
        return String.format("%s..%s", valueFrom.toString(), valueTo.toString());
    }

    /**
     * Returns a <em>from</em> value.
     * @return
     */
    public Object getValueFrom() {
        return valueFrom;
    }

    /**
     * Returns a <em>to</em> value.
     * @return
     */
    public Object getValueTo() {
        return valueTo;
    }

    /**
     * Sets <em>from</em> and <em>to</em> values.
     *
     * @param valueFrom beginning of the range (inclusive)
     * @param valueTo end of the range (inclusive)
     */
    @SuppressWarnings("HiddenField")
    public void setValues(Object valueFrom, Object valueTo) {
        this.valueFrom = valueFrom;
        this.valueTo = valueTo;
    }

    @Override
    public boolean filterEquals(AbstractFilter f) {
        if (f instanceof Range) {
            Object f2 = ((Range) f).valueFrom;
            Object t2 = ((Range) f).valueTo;
            return (Objects.equals(valueFrom, f2))
                    && (Objects.equals(valueTo, t2));
        } else {
            return false;
        }
    }
}

package ru.curs.celesta.dbutils.filter;


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
     * @param valueFrom
     * @param valueTo
     */
    public void setValues(Object valueFrom, Object valueTo) {
        this.valueFrom = valueFrom;
        this.valueTo = valueTo;
    }

    @Override
    public boolean filterEquals(AbstractFilter f) {
        if (f instanceof Range) {
            Object f2 = ((Range) f).valueFrom;
            Object t2 = ((Range) f).valueTo;
            return (valueFrom == null ? f2 == null : valueFrom.equals(f2))
                    && (valueTo == null ? t2 == null : valueTo.equals(t2));
        } else {
            return false;
        }
    }
}

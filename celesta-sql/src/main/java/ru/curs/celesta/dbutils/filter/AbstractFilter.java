package ru.curs.celesta.dbutils.filter;

/**
 * Internal filter representation on a field.
 */
public abstract class AbstractFilter {
    /**
     * Compares {@code this} filter to an other one.
     * @param f  other filter
     * @return  {@code true} if filter equal otherwise - {@code false}
     */
    public abstract boolean filterEquals(AbstractFilter f);
}


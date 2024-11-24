package ru.curs.celesta.score;

/**
 * Meta information about column of a table or a view.
 *
 * @param <V>  Java class of column value
 */
public interface ColumnMeta<V> {

    /**
     * Returns column name.
     *
     * @return
     */
    String getName();

    /**
     * Name of jdbcGetter that should be used for getting column data.
     *
     * @return
     */
    String jdbcGetterName();

    /**
     * Celesta data type that corresponds to the field.
     *
     * @return
     */
    String getCelestaType();

    /**
     * Returns corresponding Java data type.
     *
     * @return
     */
    Class<?> getJavaClass();

    /**
     * Whether the field is nullable.
     *
     * @return
     */
    boolean isNullable();

    /**
     * Column's CelestaDoc.
     *
     * @return
     */
    String getCelestaDoc();

    /**
     * Returns column ordering if any.
     *
     * @return  {@code null} if ordering is unspecified
     */
    default Ordering ordering() {
        return null;
    }

    /**
     * Returns {@code this} column meta information with ascending ordering set.
     *
     * @return
     */
    default ColumnMeta<V> asc() {
        return new ColumnMetaOrderingDecorator<>(this, Ordering.ASC);
    }

    /**
     * Returns {@code this} column meta information with descending ordering set.
     *
     * @return
     */
    default ColumnMeta<V> desc() {
        return new ColumnMetaOrderingDecorator<>(this, Ordering.DESC);
    }

    /**
     * Column ordering specifier.
     */
    enum Ordering {
        /**
         * Ascending order.
         */
        ASC,
        /**
         * Descending order.
         */
        DESC
    }

}


abstract class ColumnMetaAdaptor<V> implements ColumnMeta<V> {
    final ColumnMeta<V> column;

    ColumnMetaAdaptor(ColumnMeta<V> column) {
        this.column = column;
    }

    @Override
    public String getName() {
        return column.getName();
    }

    @Override
    public String jdbcGetterName() {
        return column.jdbcGetterName();
    }

    @Override
    public String getCelestaType() {
        return column.getCelestaType();
    }

    @Override
    public Class<?> getJavaClass() {
        return column.getJavaClass();
    }

    @Override
    public boolean isNullable() {
        return column.isNullable();
    }

    @Override
    public String getCelestaDoc() {
        return column.getCelestaDoc();
    }
}


final class ColumnMetaOrderingDecorator<V> extends ColumnMetaAdaptor<V> {
    private final Ordering ordering;

    ColumnMetaOrderingDecorator(ColumnMeta<V> column, Ordering ordering) {
        super(column);
        this.ordering = ordering;
    }

    @Override
    public Ordering ordering() {
        return ordering;
    }

    @Override
    public ColumnMetaOrderingDecorator<V> asc() {
        return (ordering != Ordering.ASC) ? new ColumnMetaOrderingDecorator<>(column, Ordering.ASC) : this;
    }

    @Override
    public ColumnMetaOrderingDecorator<V> desc() {
        return (ordering != Ordering.DESC) ? new ColumnMetaOrderingDecorator<>(column, Ordering.DESC) : this;
    }
}

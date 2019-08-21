package ru.curs.celesta.score;

/**
 * Column reference.
 *
 * @author Pavel Perminov (packpaul@mail.ru)
 *
 * @param <V>  Java class of column value
 */
public interface ColumnRef<V> {

    /**
     * Returns column name.
     *
     * @return
     */
    String getName();

    /**
     * Returns ordering for {@code this} column reference if any.
     *
     * @return
     */
    default Ordering ordering() {
        return null;
    }

    /**
     * Returns {@code this} column reference with ascending ordering.
     *
     * @return
     */
    default ColumnRef<V> asc() {
        return new ColumnRefOrderingDecorator<V>(this, Ordering.ASC);
    }

    /**
     * Returns {@code this} column reference with descending ordering.
     *
     * @return
     */
    default ColumnRef<V> desc() {
        return new ColumnRefOrderingDecorator<V>(this, Ordering.DESC);
    }

    enum Ordering {
        ASC, DESC
    }

}

class ColumnRefOrderingDecorator<V> implements ColumnRef<V> {
    private final ColumnRef<V> column;
    private final Ordering ordering;

    ColumnRefOrderingDecorator(ColumnRef<V> column, Ordering ordering) {
        this.column = column;
        this.ordering = ordering;
    }

    @Override
    public String getName() {
        return column.getName();
    }

    @Override
    public Ordering ordering() {
        return ordering;
    }

    @Override
    public ColumnRef<V> asc() {
        return (ordering != Ordering.ASC) ? new ColumnRefOrderingDecorator<V>(column, Ordering.ASC) : this;
    }

    @Override
    public ColumnRef<V> desc() {
        return (ordering != Ordering.DESC) ? new ColumnRefOrderingDecorator<V>(column, Ordering.DESC) : this;
    }
}

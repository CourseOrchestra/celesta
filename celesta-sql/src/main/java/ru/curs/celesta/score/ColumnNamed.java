package ru.curs.celesta.score;

/**
 * Information about column name.
 *
 * @author Pavel Perminov (packpaul@mail.ru)
 *
 * @param <V>  Java class of column value
 */
public interface ColumnNamed<V> {

    /**
     * Returns column name.
     *
     * @return
     */
    String getName();

}

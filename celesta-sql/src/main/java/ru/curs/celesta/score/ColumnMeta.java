package ru.curs.celesta.score;

/**
 * Information about column type of a table or a view.
 */
public interface ColumnMeta {

    /**
     * Name of jdbcGetter that should be used for getting column data.
     */
    String jdbcGetterName();

    /**
     * Celesta data type that corresponds to the field.
     */
    String getCelestaType();

    /**
     * Returns corresponding Java data type.
     * @return
     */
    Class<?> getJavaClass();

    /**
     * Whether the field is nullable.
     */
    boolean isNullable();

    /**
     * Column's CelestaDoc.
     */
    String getCelestaDoc();

}

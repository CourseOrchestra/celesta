package ru.curs.celesta.score;

import java.util.Map;

public interface HasColumns {
    /**
     * List of columns with names.
     */
    Map<String, ? extends ColumnMeta<?>> getColumns();

    /**
     * Column index in the list of columns.
     *
     * @param name  column name.
     */
    int getColumnIndex(String name);
}

package ru.curs.celesta.score;

import java.util.Map;

public interface HasColumns {

    /**
     * Перечень столбцов с именами.
     */
    Map<String, ? extends ColumnMeta> getColumns();

    /**
     * Номер столбца в перечне столбцов.
     *
     * @param name
     *            Имя столбца.
     */
    int getColumnIndex(String name);
}

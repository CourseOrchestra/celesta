package ru.curs.celesta.dbutils.adaptors.column;

import ru.curs.celesta.score.Column;

/**
 * Класс, ответственный за генерацию определения столбца таблицы в разных СУБД.
 */
//TODO: Javadoc In English
public abstract class ColumnDefiner {
    static final String DEFAULT = "default ";

    //TODO: Javadoc
    public abstract String dbFieldType();

    /**
     * Возвращает определение колонки, содержащее имя, тип и NULL/NOT NULL (без
     * DEFAULT). Требуется для механизма изменения колонок.
     *
     * @param c колонка.
     */
    //TODO: Javadoc In English
    public abstract String getMainDefinition(Column c);

    /**
     * Отдельно возвращает DEFAULT-определение колонки.
     *
     * @param c колонка.
     */
    //TODO: Javadoc In English
    public abstract String getDefaultDefinition(Column c);

    /**
     * Возвращает полное определение колонки (для создания колонки).
     *
     * @param c колонка
     */
    //TODO: Javadoc In English
    public String getFullDefinition(Column c) {
        return join(getMainDefinition(c), getDefaultDefinition(c));
    }

    //TODO: Javadoc
    public String nullable(Column c) {
        return c.isNullable() ? "null" : "not null";
    }

    /**
     * Соединяет строки через пробел.
     *
     * @param ss массив строк для соединения в виде свободного параметра.
     */
    //TODO: Javadoc In English
    public static String join(String... ss) {
        StringBuilder sb = new StringBuilder();
        boolean multiple = false;
        for (String s : ss)
            if (!"".equals(s)) {
                if (multiple)
                    sb.append(' ' + s);
                else {
                    sb.append(s);
                    multiple = true;
                }
            }
        return sb.toString();
    }

}

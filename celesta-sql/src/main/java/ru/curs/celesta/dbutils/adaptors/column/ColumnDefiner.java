package ru.curs.celesta.dbutils.adaptors.column;

import ru.curs.celesta.score.Column;

/**
 * Class responsible for generation of table column definition in different DBMS.
 */
public abstract class ColumnDefiner {
    static final String DEFAULT = "default ";

    /**
     * Returns column field type, e.g. "boolean", "double","int" etc.
     * depending on the DBMS in question.
     *
     */
    public abstract String dbFieldType();

    /**
     * Returns column definition that contains name, type and NULL/NOT NULL (
     * without DEFAULT). It is needed for the column change mechanism.
     *
     * @param c  column.
     */
    public abstract String getMainDefinition(Column<?> c);

    /**
     * Returns separately DEFAULT definition of the column.
     *
     * @param c  column.
     */
    public abstract String getDefaultDefinition(Column<?> c);

    /**
     * Returns full definition of the column (for column creation).
     *
     * @param c  column.
     */
    public String getFullDefinition(Column<?> c) {
        return join(getMainDefinition(c), getDefaultDefinition(c));
    }

    /**
     * Whether the column is nullable.
     *
     * @param c  column.
     * @return  "null" | "not null"
     */
    public String nullable(Column<?> c) {
        return c.isNullable() ? "null" : "not null";
    }

    /**
     * Concatenates strings separated by a space symbol.
     *
     * @param ss  strings array for concatenation in form of a free parameter.
     */
    public static String join(String... ss) {
        StringBuilder sb = new StringBuilder();
        boolean multiple = false;
        for (String s : ss) {
            if (!"".equals(s)) {
                if (multiple) {
                    sb.append(' ' + s);
                } else {
                    sb.append(s);
                    multiple = true;
                }
            }
        }
        return sb.toString();
    }

}

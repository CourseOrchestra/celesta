package ru.curs.celesta.dbutils.adaptors.column;

import ru.curs.celesta.score.Column;

/**
 * Base column definer for Firebird.
 */
public abstract class FireBirdColumnDefiner extends ColumnDefiner {

    public abstract String getInternalDefinition(Column c);

    /**
     * Returns full definition of the column (for column creation).
     *
     * @param c column.
     */
    @Override
    public String getFullDefinition(Column c) {
        return join(getInternalDefinition(c), getDefaultDefinition(c), nullable(c));
    }

    @Override
    public final String getMainDefinition(Column c) {
        return join(getInternalDefinition(c), nullable(c));
    }

    /**
     * Whether the column is nullable.
     *
     * @param c  column.
     * @return  "null" | "not null"
     */
    @Override
    public String nullable(Column c) {
        return c.isNullable() ? "" : "not null";
    }
}

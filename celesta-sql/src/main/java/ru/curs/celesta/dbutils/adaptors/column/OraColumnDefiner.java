package ru.curs.celesta.dbutils.adaptors.column;

import ru.curs.celesta.score.Column;

/**
 * Column definer for Oracle that takes into account the fact that in Oracle
 * DEFAULT should precede NOT NULL.
 */
public abstract class OraColumnDefiner extends ColumnDefiner {
    public abstract String getInternalDefinition(Column<?> c);

    @Override
    public String getFullDefinition(Column<?> c) {
        return join(getInternalDefinition(c), getDefaultDefinition(c), nullable(c));
    }

    @Override
    public final String getMainDefinition(Column<?> c) {
        return join(getInternalDefinition(c), nullable(c));
    }

}

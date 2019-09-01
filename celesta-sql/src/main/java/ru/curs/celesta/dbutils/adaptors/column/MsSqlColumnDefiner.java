package ru.curs.celesta.dbutils.adaptors.column;

import ru.curs.celesta.score.Column;

/**
 * Column definer for MsSql.
 */
public abstract class MsSqlColumnDefiner extends ColumnDefiner {
    public abstract String getLightDefaultDefinition(Column<?> c);

    String msSQLDefault(Column<?> c) {
        return String.format("constraint \"def_%s_%s\" ", c.getParentTable().getName(), c.getName())
                + DEFAULT;
    }
}

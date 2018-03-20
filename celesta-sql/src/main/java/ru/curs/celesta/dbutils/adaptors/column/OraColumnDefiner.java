package ru.curs.celesta.dbutils.adaptors.column;

import ru.curs.celesta.score.Column;

/**
 * Определитель колонок для Oracle, учитывающий тот факт, что в Oracle
 * DEFAULT должен идти до NOT NULL.
 */
public abstract class OraColumnDefiner extends ColumnDefiner {
    public abstract String getInternalDefinition(Column c);

    @Override
    public String getFullDefinition(Column c) {
        return join(getInternalDefinition(c), getDefaultDefinition(c), nullable(c));
    }

    @Override
    public final String getMainDefinition(Column c) {
        return join(getInternalDefinition(c), nullable(c));
    }
}
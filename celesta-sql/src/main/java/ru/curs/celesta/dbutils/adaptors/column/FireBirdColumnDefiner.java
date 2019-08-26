package ru.curs.celesta.dbutils.adaptors.column;

import ru.curs.celesta.score.Column;

public abstract class FireBirdColumnDefiner extends ColumnDefiner {

    public abstract String getInternalDefinition(Column c);

    @Override
    public String getFullDefinition(Column c) {
        return join(getInternalDefinition(c), getDefaultDefinition(c), nullable(c));
    }

    @Override
    public final String getMainDefinition(Column c) {
        return join(getInternalDefinition(c), nullable(c));
    }

    @Override
    public String nullable(Column c) {
        return c.isNullable() ? "" : "not null";
    }
}

package ru.curs.celesta.dbutils.adaptors.ddl;

import ru.curs.celesta.dbutils.adaptors.DBAdaptor;

import static ru.curs.celesta.dbutils.adaptors.constants.CommonConstants.*;
import static ru.curs.celesta.dbutils.adaptors.constants.OpenSourceConstants.*;

import ru.curs.celesta.dbutils.adaptors.column.ColumnDefinerFactory;
import ru.curs.celesta.dbutils.meta.DbColumnInfo;
import ru.curs.celesta.dbutils.meta.DbIndexInfo;
import ru.curs.celesta.event.TriggerQuery;
import ru.curs.celesta.score.*;

import java.sql.Connection;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * Base class for SQL generation of data definition of open source DBs (PostgreSQL, H2).
 */
public abstract class OpenSourceDdlGenerator extends DdlGenerator {

    public OpenSourceDdlGenerator(DBAdaptor dmlAdaptor) {
        super(dmlAdaptor);
    }

    @Override
    final List<String> dropIndex(Grain g, DbIndexInfo dBIndexInfo) {
        String sql = dropIndex(g.getName(), dBIndexInfo.getIndexName());
        String sql2 = dropIndex(
                g.getName(),
                dBIndexInfo.getIndexName() + CONJUGATE_INDEX_POSTFIX
        );

        return Arrays.asList(sql, sql2);
    }

    @Override
    final String dropTriggerSql(TriggerQuery query) {
        String sql = String.format(
                "DROP TRIGGER \"%s\" ON %s",
                query.getName(), tableString(query.getSchema(), query.getTableName())
        );
        return sql;
    }

    @Override
    final List<String> updateColumn(Connection conn, Column<?> c, DbColumnInfo actual) {
        @SuppressWarnings("unchecked")
        final Class<? extends Column<?>> cClass = (Class<Column<?>>) c.getClass();

        List<String> result = new LinkedList<>();
        // Starting with deletion of default-value
        String sql = String.format(
                ALTER_TABLE + tableString(c.getParentTable().getGrain().getName(), c.getParentTable().getName())
                        + " ALTER COLUMN \"%s\" DROP DEFAULT", c.getName()
        );
        result.add(sql);

        updateColType(c, actual, result);

        // Checking for nullability
        if (c.isNullable() != actual.isNullable()) {
            sql = String.format(
                    ALTER_TABLE + tableString(c.getParentTable().getGrain().getName(), c.getParentTable().getName())
                            + " ALTER COLUMN \"%s\" %s",
                    c.getName(), c.isNullable() ? "DROP NOT NULL" : "SET NOT NULL");
            result.add(sql);
        }

        // If there's an empty default in data, and non-empty one in metadata then
        if (c.getDefaultValue() != null || c instanceof DateTimeColumn && ((DateTimeColumn) c).isGetdate()
                || c instanceof IntegerColumn && ((IntegerColumn) c).getSequence() != null) {
            sql = String.format(
                    ALTER_TABLE + tableString(c.getParentTable().getGrain().getName(), c.getParentTable().getName())
                            + " ALTER COLUMN \"%s\" SET %s",
                    c.getName(), ColumnDefinerFactory.getColumnDefiner(getType(), cClass).getDefaultDefinition(c));
            result.add(sql);
        }

        return result;
    }

    @Override
    final Optional<String> dropAutoIncrement(Connection conn, TableElement t) {
        String sql = String.format("drop sequence if exists \"%s\".\"%s_seq\"", t.getGrain().getName(), t.getName());
        return Optional.of(sql);
    }

    abstract void updateColType(Column<?> c, DbColumnInfo actual, List<String> batch);

    private String dropIndex(String schemaName, String indexName) {
        return String.format(
                "DROP INDEX IF EXISTS %s", tableString(schemaName, indexName)
        );
    }

}

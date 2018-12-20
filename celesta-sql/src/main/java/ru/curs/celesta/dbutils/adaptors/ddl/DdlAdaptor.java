package ru.curs.celesta.dbutils.adaptors.ddl;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.meta.DbColumnInfo;
import ru.curs.celesta.dbutils.meta.DbIndexInfo;
import ru.curs.celesta.event.TriggerQuery;
import ru.curs.celesta.score.*;

import java.sql.Connection;
import java.util.List;
import java.util.Optional;


public final class DdlAdaptor {
    private final DdlGenerator ddlGenerator;
    private final DdlConsumer ddlConsumer;

    public DdlAdaptor(DdlGenerator ddlGenerator, DdlConsumer ddlConsumer) {
        this.ddlGenerator = ddlGenerator;
        this.ddlConsumer = ddlConsumer;
    }

    public final void createSchema(Connection conn, String name)  {
        Optional<String> sql = ddlGenerator.createSchema(name);
        processSql(conn, sql);
    }

    public final void dropView(Connection conn, String schemaName, String viewName)  {
        String sql = ddlGenerator.dropView(schemaName, viewName);
        processSql(conn, sql);
    }

    public final void dropParameterizedView(Connection conn, String schemaName, String viewName)  {
        List<String> sqlList = ddlGenerator.dropParameterizedView(schemaName, viewName, conn);
        processSql(conn, sqlList);
    }

    public final void dropIndex(Connection conn, Grain g, DbIndexInfo dBIndexInfo)  {
        List<String> sqlList = ddlGenerator.dropIndex(g, dBIndexInfo);
        processSql(conn, sqlList);
    }

    public final void dropFK(Connection conn, String schemaName, String tableName, String fkName)  {
        String sql = ddlGenerator.dropFk(schemaName, tableName, fkName);
        try {
            processSql(conn, sql);
        } catch (CelestaException e) {
            throw new CelestaException("Cannot drop foreign key '%s': %s", fkName, e.getMessage());
        }

        List<String> sqlList = ddlGenerator.dropUpdateRule(fkName);
        try {
            processSql(conn, sqlList);
        } catch (CelestaException e) {
            //do nothing
        }

    }

    public final void dropTrigger(Connection conn, TriggerQuery query)  {
        String sql = ddlGenerator.dropTrigger(query);
        processSql(conn, sql);
    }


    public final void createSequence(Connection conn, SequenceElement s)  {
        String sql = ddlGenerator.createSequence(s);

        try {
            processSql(conn, sql);
        } catch (CelestaException e) {
            throw new CelestaException("Error while creating sequence %s.%s: %s", s.getGrain().getName(), s.getName(),
                    e.getMessage());
        }
    }

    public final void alterSequence(Connection conn, SequenceElement s)  {
        String sql = ddlGenerator.alterSequence(s);

        try {
            processSql(conn, sql);
        } catch (CelestaException e) {
            throw new CelestaException("Error while altering sequence %s.%s: %s", s.getGrain().getName(), s.getName(),
                    e.getMessage());
        }
    }

    public final void createTable(Connection conn, TableElement te)  {
        String sql = ddlGenerator.createTable(te);

        try {
            processSql(conn, sql);
            processSql(conn, "COMMIT");
            List<String> sqlList;
            sqlList = ddlGenerator.updateVersioningTrigger(conn, te);
            processSql(conn, sqlList);
            sqlList = ddlGenerator.afterCreateTable(te);
            processSql(conn, sqlList);
        } catch (CelestaException e) {
            throw new CelestaException("Error of creating %s: %s", te.getName(), e.getMessage());
        }
    }

    public final void updateVersioningTrigger(Connection conn, TableElement t)  {
        List<String> sqlList = ddlGenerator.updateVersioningTrigger(conn, t);
        processSql(conn, sqlList);
    }

    public final void dropPk(Connection conn, TableElement t, String pkName)  {
        String sql = ddlGenerator.dropPk(t, pkName);

        try {
            processSql(conn, sql);
        } catch (CelestaException e) {
            throw new CelestaException(
                    String.format("Cannot drop PK '%s': %s", pkName, e.getMessage()), e
            );
        }
    }

    public final void updateColumn(Connection conn, Column c, DbColumnInfo actual)  {
        List<String> sqlList = ddlGenerator.updateColumn(conn, c, actual);

        try {
            processSql(conn, sqlList);
        } catch (CelestaException e) {
            throw new CelestaException(
                    String.format(
                            "Cannot modify column %s on table %s.%s: %s", c.getName(),
                            c.getParentTable().getGrain().getName(), c.getParentTable().getName(), e.getMessage()
                    ), e
            );
        }
    }

    /**
     * Добавляет к таблице новую колонку.
     *
     * @param conn Соединение с БД.
     * @param c    Колонка для добавления.
     */
    //TODO: Javadoc In English
    public final void createColumn(Connection conn, Column c)  {
        String sql = ddlGenerator.createColumn(c);
        try {
            processSql(conn, sql);
        } catch (CelestaException e) {
            throw new CelestaException(
                    String.format(
                            "Error of creating %s.%s: %s",
                            c.getParentTable().getName(), c.getName(), e.getMessage()
                    ), e
            );
        }
    }


    /**
     * Создаёт первичный ключ на таблице в соответствии с метаописанием.
     *
     * @param conn Соединение с базой данных.
     * @param t    Таблица.
     */
    //TODO: Javadoc In English
    public final void createPk(Connection conn, TableElement t)  {
        String sql = ddlGenerator.createPk(t);

        try {
            processSql(conn, sql);
        } catch (CelestaException e) {
            throw new CelestaException(
                    String.format(
                            "Cannot create PK '%s': %s", t.getPkConstraintName(), e.getMessage()
                    ), e
            );
        }
    }


    /**
     * Создаёт в грануле индекс на таблице.
     *
     * @param conn  Соединение с БД.
     * @param index описание индекса.
     */
    //TODO: Javadoc In English
    public final void createIndex(Connection conn, Index index)  {
        List<String> sqlList = ddlGenerator.createIndex(index);

        try {
            processSql(conn, sqlList);
            processSql(conn, "COMMIT");
        } catch (CelestaException e) {
            throw new CelestaException("Cannot create index '%s': %s", index.getName(), e.getMessage());
        }
    }

    /**
     * Создаёт первичный ключ.
     *
     * @param conn соединение с БД.
     * @param fk   первичный ключ
     */
    //TODO: Javadoc In English
    public final void createFk(Connection conn, ForeignKey fk)  {
        try {
            List<String> sqlList = ddlGenerator.createFk(conn, fk);
            for (String slq : sqlList) {
                processSql(conn, slq);
            }
        } catch (CelestaException e) {
            throw new CelestaException("Cannot create foreign key '%s': %s", fk.getConstraintName(),
                    e.getMessage());
        }
    }

    /**
     * Создаёт представление в базе данных на основе метаданных.
     *
     * @param conn Соединение с БД.
     * @param v    Представление.
     */
    //TODO: Javadoc In English
    public final void createView(Connection conn, View v)  {
        String sql = this.ddlGenerator.createView(v);

        try {
            processSql(conn, sql);
        } catch (CelestaException e) {
            throw new CelestaException("Error while creating view %s.%s: %s", v.getGrain().getName(), v.getName(),
                    e.getMessage());

        }
    }

    public final SQLGenerator getViewSQLGenerator() {
        return this.ddlGenerator.getViewSQLGenerator();
    }

    //TODO: Javadoc
    public final void createParameterizedView(Connection conn, ParameterizedView pv)  {
        List<String> sqlList = this.ddlGenerator.createParameterizedView(pv);

        try {
            processSql(conn, sqlList);
        } catch (CelestaException e) {
            throw new CelestaException(
                    String.format(
                            "Error while creating parameterized view %s.%s: %s",
                            pv.getGrain().getName(), pv.getName(), e.getMessage()
                    ), e
            );
        }
    }


    /**
     * Deletes table from RDBMS.
     *
     * @param conn Connection to use.
     * @param t    TableElement metadata of deleting table provided by Celesta.
     */
    public final void dropTable(Connection conn, TableElement t)  {
        String sql = this.ddlGenerator.dropTable(t);
        processSql(conn, sql);
        Optional<String> sqlOpt = this.ddlGenerator.dropAutoIncrement(conn, t);
        processSql(conn, sqlOpt);
        processSql(conn, "COMMIT");
    }

    //TODO: Javadoc
    public final void initDataForMaterializedView(Connection conn, MaterializedView mv)  {
        List<String> sqlList = this.ddlGenerator.initDataForMaterializedView(mv);

        try {
            processSql(conn, sqlList);
        } catch (CelestaException e) {
            throw new CelestaException("Can't init data for materialized view %s.%s: %s",
                    mv.getGrain().getName(), mv.getName(), e);
        }

    }

    //TODO: Javadoc
    public void dropTableTriggersForMaterializedViews(Connection conn, Table t)  {
        List<String> sqlList = this.ddlGenerator.dropTableTriggersForMaterializedViews(conn, t);
        try {
            processSql(conn, sqlList);
        } catch (CelestaException e) {
            throw new CelestaException("Can't drop triggers for materialized views", e);
        }
    }

    public void createTableTriggersForMaterializedViews(Connection conn, Table t)  {
        List<String> sqlList = this.ddlGenerator.createTableTriggersForMaterializedViews(t);
        try {
            processSql(conn, sqlList);
        } catch (CelestaException e) {
            throw new CelestaException(
                    String.format(
                            "Could not update triggers on %s.%s for materialized views: %s",
                            t.getGrain().getName(), t.getName(), e.getMessage()
                    ), e
            );
        }
    }

    public void executeNative(Connection conn, String sql)  {
        processSql(conn, sql);
    }

    private void processSql(Connection conn, Optional<String> sql)  {
        if (sql.isPresent()) {
            processSql(conn, sql.get());
        }
    }

    private void processSql(Connection conn, String sql)  {
        ddlConsumer.consume(conn, sql);
    }

    private void processSql(Connection conn, List<String> sqlList)  {
        for (String sql : sqlList) {
            processSql(conn, sql);
        }
    }
}

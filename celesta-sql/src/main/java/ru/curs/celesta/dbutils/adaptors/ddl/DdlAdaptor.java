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

    /**
     * Creates DB schema.
     *
     * @param conn  DB connection
     * @param name  schema name
     */
    public void createSchema(Connection conn, String name)  {
        Optional<String> sql = ddlGenerator.createSchema(name);
        processSql(conn, sql);
    }

    /**
     * Drops view from a DB schema.
     *
     * @param conn  DB connection
     * @param schemaName  schema name
     * @param viewName  view name
     */
    public void dropView(Connection conn, String schemaName, String viewName)  {
        String sql = ddlGenerator.dropView(schemaName, viewName);
        processSql(conn, sql);
    }

    public void dropParameterizedView(Connection conn, String schemaName, String viewName)  {
        List<String> sqlList = ddlGenerator.dropParameterizedView(schemaName, viewName, conn);
        processSql(conn, sqlList);
    }

    /**
     * Drops index of a grain.
     *
     * @param conn  DB connection
     * @param g  grain
     * @param dBIndexInfo  index information
     */
    public void dropIndex(Connection conn, Grain g, DbIndexInfo dBIndexInfo)  {
        List<String> sqlList = ddlGenerator.dropIndex(g, dBIndexInfo);
        processSql(conn, sqlList);
    }

    /**
     * Drops foreign key of table in a scheme.
     *
     * @param conn       DB connection
     * @param schemaName grain name
     * @param tableName  table name for column(s) of which FK is declared
     * @param fkName     name of foreign key
     */
    public void dropFK(Connection conn, String schemaName, String tableName, String fkName)  {
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

    /**
     * Drops a trigger from DB.
     *
     * @param conn  Connection
     * @param query  Trigger query
     */
    public void dropTrigger(Connection conn, TriggerQuery query)  {
        String sql = ddlGenerator.dropTrigger(query);
        processSql(conn, sql);
    }

    /**
     * Creates a sequence in the database.
     *
     * @param conn  DB connection
     * @param s  sequence element
     */
    public void createSequence(Connection conn, SequenceElement s)  {
        String sql = ddlGenerator.createSequence(s);

        try {
            processSql(conn, sql);
        } catch (CelestaException e) {
            throw new CelestaException("Error while creating sequence %s.%s: %s", s.getGrain().getName(), s.getName(),
                    e.getMessage());
        }
    }

    /**
     * Alters sequence in the database.
     *
     * @param conn DB connection
     * @param s sequence element
     */
    public void alterSequence(Connection conn, SequenceElement s)  {
        String sql = ddlGenerator.alterSequence(s);

        try {
            processSql(conn, sql);
        } catch (CelestaException e) {
            throw new CelestaException("Error while altering sequence %s.%s: %s", s.getGrain().getName(), s.getName(),
                    e.getMessage());
        }
    }

    /**
     * Creates a table "from scratch" in the database.
     *
     * @param conn Connection
     * @param te   Table for creation (accepts also table in case if such table exists)
     */
    public void createTable(Connection conn, TableElement te)  {
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

    public void updateVersioningTrigger(Connection conn, TableElement t)  {
        List<String> sqlList = ddlGenerator.updateVersioningTrigger(conn, t);
        processSql(conn, sqlList);
    }

    /**
     * Drops primary key from the table by using known name of the primary key.
     *
     * @param conn  DB connection
     * @param t  table
     * @param pkName  primary key name
     */
    public void dropPk(Connection conn, TableElement t, String pkName)  {
        String sql = ddlGenerator.dropPk(t, pkName);

        try {
            processSql(conn, sql);
        } catch (CelestaException e) {
            throw new CelestaException(
                    String.format("Cannot drop PK '%s': %s", pkName, e.getMessage()), e
            );
        }
    }

    /**
     * Updates a table column.
     *
     * @param conn    DB connection
     * @param c       Column to update
     * @param actual  Actual column info
     */
    public void updateColumn(Connection conn, Column c, DbColumnInfo actual)  {
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
     * Adds a new column to the table.
     *
     * @param conn  DB connection
     * @param c  column
     */
    public void createColumn(Connection conn, Column c) {
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
     * Creates primary key in the table according to meta description.
     *
     * @param conn  database connection
     * @param t     table
     */
    public void createPk(Connection conn, TableElement t)  {
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
     * Creates a table index in the grain.
     *
     * @param conn   DB connection
     * @param index  index description
     */
    public void createIndex(Connection conn, Index index)  {
        List<String> sqlList = ddlGenerator.createIndex(index);

        try {
            processSql(conn, sqlList);
            processSql(conn, "COMMIT");
        } catch (CelestaException e) {
            throw new CelestaException("Cannot create index '%s': %s", index.getName(), e.getMessage());
        }
    }

    /**
     * Creates foreign key in the DB.
     *
     * @param conn  DB connection
     * @param fk    foreign key from score
     */
    public void createFk(Connection conn, ForeignKey fk)  {
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
     * Creates a view in the database from metadata.
     *
     * @param conn  DB connection
     * @param v     View from scrore
     */
    public void createView(Connection conn, View v)  {
        String sql = this.ddlGenerator.createView(v);

        try {
            processSql(conn, sql);
        } catch (CelestaException e) {
            throw new CelestaException("Error while creating view %s.%s: %s", v.getGrain().getName(), v.getName(),
                    e.getMessage());

        }
    }

    public SQLGenerator getViewSQLGenerator() {
        return this.ddlGenerator.getViewSQLGenerator();
    }

    //TODO: Javadoc
    public void createParameterizedView(Connection conn, ParameterizedView pv)  {
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
     * @param conn  Connection to use.
     * @param t     TableElement metadata of deletable table provided by Celesta.
     */
    public void dropTable(Connection conn, TableElement t)  {
        String sql = this.ddlGenerator.dropTable(t);
        processSql(conn, sql);
        Optional<String> sqlOpt = this.ddlGenerator.dropAutoIncrement(conn, t);
        processSql(conn, sqlOpt);
        processSql(conn, "COMMIT");
    }

    //TODO: Javadoc
    public void initDataForMaterializedView(Connection conn, MaterializedView mv)  {
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

    /**
     * Executes native SQL query.
     *
     * @param conn  DB connection
     * @param sql   SQL to execute
     */
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

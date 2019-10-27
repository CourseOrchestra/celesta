package ru.curs.celesta.dbutils.adaptors.ddl;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.DBType;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.dbutils.adaptors.column.ColumnDefinerFactory;
import ru.curs.celesta.dbutils.meta.DbColumnInfo;
import ru.curs.celesta.dbutils.meta.DbIndexInfo;
import ru.curs.celesta.event.TriggerQuery;
import ru.curs.celesta.score.BasicTable;
import ru.curs.celesta.score.BinaryColumn;
import ru.curs.celesta.score.BooleanColumn;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.Count;
import ru.curs.celesta.score.DateTimeColumn;
import ru.curs.celesta.score.DecimalColumn;
import ru.curs.celesta.score.Expr;
import ru.curs.celesta.score.FloatingColumn;
import ru.curs.celesta.score.ForeignKey;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.Index;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.MaterializedView;
import ru.curs.celesta.score.ParameterizedView;
import ru.curs.celesta.score.SQLGenerator;
import ru.curs.celesta.score.SequenceElement;
import ru.curs.celesta.score.StringColumn;
import ru.curs.celesta.score.Sum;
import ru.curs.celesta.score.TableElement;
import ru.curs.celesta.score.VersionedElement;
import ru.curs.celesta.score.View;
import ru.curs.celesta.score.ZonedDateTimeColumn;


import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.SQLException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static ru.curs.celesta.dbutils.adaptors.constants.CommonConstants.ALTER_TABLE;

/**
 * Base class for SQL generation of data definition.
 */
public abstract class DdlGenerator {

    static final Map<String, Class<? extends Column<?>>> CELESTA_TYPES_COLUMN_CLASSES = new HashMap<>();

    static {
        CELESTA_TYPES_COLUMN_CLASSES.put(IntegerColumn.CELESTA_TYPE, IntegerColumn.class);
        CELESTA_TYPES_COLUMN_CLASSES.put(FloatingColumn.CELESTA_TYPE, FloatingColumn.class);
        CELESTA_TYPES_COLUMN_CLASSES.put(DecimalColumn.CELESTA_TYPE, DecimalColumn.class);
        CELESTA_TYPES_COLUMN_CLASSES.put(BooleanColumn.CELESTA_TYPE, BooleanColumn.class);
        CELESTA_TYPES_COLUMN_CLASSES.put(StringColumn.VARCHAR, StringColumn.class);
        CELESTA_TYPES_COLUMN_CLASSES.put(BinaryColumn.CELESTA_TYPE, BinaryColumn.class);
        CELESTA_TYPES_COLUMN_CLASSES.put(DateTimeColumn.CELESTA_TYPE, DateTimeColumn.class);
        CELESTA_TYPES_COLUMN_CLASSES.put(ZonedDateTimeColumn.CELESTA_TYPE, ZonedDateTimeColumn.class);
    }

    DBAdaptor dmlAdaptor;

    Map<String, Map<String, Set<String>>> triggers = new HashMap<>();

    public DdlGenerator(DBAdaptor dmlAdaptor) {
        this.dmlAdaptor = dmlAdaptor;
    }

    /**
     * Generates SQL for schema creation in the DB.
     *
     * @param name  schema names
     * @return
     */
    Optional<String> createSchema(String name) {
        String sql = String.format("create schema \"%s\"", name);
        return Optional.of(sql);
    }


    /**
     * Generates SQL for sequence creation in the DB.
     *
     * @param s sequence definition
     * @return
     */
    List<String> createSequence(SequenceElement s) {
        String sql = String.format(
            "CREATE SEQUENCE %s %s",
            sequenceString(s.getGrain().getName(), s.getName()),
            generateArgumentsForCreateSequenceExpression(s)
        );

        return Arrays.asList(sql);
    }

    /**
     * Generates SQL for dropping view in the schema.
     *
     * @param schemaName  schema name
     * @param viewName  view name
     * @return
     */
    String dropView(String schemaName, String viewName) {
        String sql = String.format("DROP VIEW %s", tableString(schemaName, viewName));
        return sql;
    }

    abstract List<String> dropParameterizedView(
            String schemaName, String viewName, Connection conn
    );

    abstract List<String> dropIndex(Grain g, DbIndexInfo dBIndexInfo);

    final String dropFk(String schemaName, String tableName, String fkName) {
        String sql = String.format(
                "alter table %s drop constraint \"%s\"",
                tableString(schemaName, tableName), fkName
        );

        return sql;
    }

    List<String> dropUpdateRule(String fkName) {
        return Collections.emptyList();
    }

    final String dropTrigger(TriggerQuery query) {
        this.forgetTrigger(query);
        return dropTriggerSql(query);
    }

    abstract String dropTriggerSql(TriggerQuery query);

    final String tableString(String schemaName, String tableName) {
        return this.dmlAdaptor.tableString(schemaName, tableName);
    }

    final String sequenceString(String schemaName, String sequenceName) {
        return this.dmlAdaptor.sequenceString(schemaName, sequenceName);
    }

    final String pkConstraintString(TableElement tableElement) {
        return this.dmlAdaptor.pkConstraintString(tableElement);
    }

    protected List<String> alterSequence(SequenceElement s) {
        String sql = String.format(
                "ALTER SEQUENCE %s %s",
                sequenceString(s.getGrain().getName(), s.getName()),
                generateArgumentsForCreateSequenceExpression(s, SequenceElement.Argument.START_WITH)
        );

        return Arrays.asList(sql);
    }

    String generateArgumentsForCreateSequenceExpression(
            SequenceElement s, SequenceElement.Argument... excludedArguments) {

        return s.getArguments().entrySet().stream()
                .filter(e -> !Arrays.asList(excludedArguments).contains(e.getKey()))
                .map(
                        e -> e.getKey().getSql(e.getValue())
                ).collect(Collectors.joining());
    }

    /**
     * Returns String representation of table definition for RDBMS.
     *
     * @param te TableElement metadata provided by Celesta.
     * @return Returns String representation of table definition for RDBMS.
     */
    String createTable(TableElement te) {
        StringBuilder sb = new StringBuilder();
        // Table definition with columns
        sb.append(
                "create table " + tableString(te.getGrain().getName(), te.getName()) + "(\n"
        );
        boolean multiple = false;
        for (Column<?> c : te.getColumns().values()) {
            if (multiple) {
                sb.append(",\n");
            }
            sb.append("  " + columnDef(c));
            multiple = true;
        }

        if (te instanceof VersionedElement) {
            VersionedElement ve = (VersionedElement) te;
            // For versioned tables, the "recversion" column
            if (ve.isVersioned()) {
                sb.append(",\n").append("  " + columnDef(ve.getRecVersionField()));
            }
        }

        if (te.hasPrimeKey()) {
            sb.append(",\n");
            // Primary key definition if it should be present in the table
            sb.append(String.format("  constraint \"%s\" primary key (", pkConstraintString(te)));
            multiple = false;
            for (String s : te.getPrimaryKey().keySet()) {
                if (multiple) {
                    sb.append(", ");
                }
                sb.append('"');
                sb.append(s);
                sb.append('"');
                multiple = true;
            }
            sb.append(")");
        }

        sb.append("\n)");

        return sb.toString();
    }

    /**
     * Generates SQL for dropping primary key from the table by using
     * known name of the primary key.
     *
     * @param t  table table name
     * @param pkName  primary key name
     * @return
     */
    public abstract String dropPk(TableElement t, String pkName);


    /**
     * Returns String representation of column definition for RDBMS.
     *
     * @param c Column metadata provided by Celesta.
     * @return Returns String representation of column definition for RDBMS.
     */
    //TODO:Must be defined in single place
    final String columnDef(Column<?> c) {
        @SuppressWarnings("unchecked")
        final Class<? extends Column<?>> cClass = (Class<Column<?>>) c.getClass();

        return ColumnDefinerFactory
                .getColumnDefiner(getType(), cClass)
                .getFullDefinition(c);
    }

    final String createColumn(Column<?> c) {
        String sql = String.format(ALTER_TABLE
                + tableString(c.getParentTable().getGrain().getName(), c.getParentTable().getName())
                + " add %s", columnDef(c));
        return sql;
    }

    abstract DBType getType();

    /**
     * Generates SQL for versioning trigger update on a table.
     *
     * @param conn Connection
     * @param t    Table (versioned or unversioned)
     * @throws RuntimeException  on trigger creation or deletion
     */
    abstract List<String> updateVersioningTrigger(Connection conn, TableElement t);

    abstract List<String> createIndex(Index index);

    /**
     * Alters a table column.
     *
     * @param c  column for update
     * @throws RuntimeException  on column update error
     */
    abstract List<String> updateColumn(Connection conn, Column<?> c, DbColumnInfo actual);

    final String createPk(TableElement t) {
        StringBuilder sb = new StringBuilder();

        sb.append(
                String.format(
                        "alter table %s add constraint \"%s\" " + " primary key (",
                        tableString(t.getGrain().getName(), t.getName()), pkConstraintString(t)
                )
        );

        boolean multiple = false;
        for (String s : t.getPrimaryKey().keySet()) {
            if (multiple) {
                sb.append(", ");
            }
            sb.append('"');
            sb.append(s);
            sb.append('"');
            multiple = true;
        }
        sb.append(")");

        return sb.toString();
    }

    final List<String> createFk(Connection conn, ForeignKey fk)  {
        LinkedList<StringBuilder> sqlQueue = new LinkedList<>();

        // Building a query for FK creation
        StringBuilder sql = new StringBuilder();
        sql.append(ALTER_TABLE);
        sql.append(tableString(fk.getParentTable().getGrain().getName(), fk.getParentTable().getName()));
        sql.append(" add constraint \"");
        sql.append(fk.getConstraintName());
        sql.append("\" foreign key (");
        boolean needComma = false;
        for (String name : fk.getColumns().keySet()) {
            if (needComma) {
                sql.append(", ");
            }
            sql.append('"');
            sql.append(name);
            sql.append('"');
            needComma = true;
        }
        sql.append(") references ");
        sql.append(tableString(fk.getReferencedTable().getGrain().getName(),
                fk.getReferencedTable().getName()));
        sql.append("(");
        needComma = false;
        for (String name : fk.getReferencedTable().getPrimaryKey().keySet()) {
            if (needComma) {
                sql.append(", ");
            }
            sql.append('"');
            sql.append(name);
            sql.append('"');
            needComma = true;
        }
        sql.append(")");

        switch (fk.getDeleteRule()) {
            case SET_NULL:
                sql.append(" on delete set null");
                break;
            case CASCADE:
                sql.append(" on delete cascade");
                break;
            case NO_ACTION:
            default:
                break;
        }

        sqlQueue.add(sql);
        processCreateUpdateRule(conn, fk, sqlQueue);

        return sqlQueue.stream()
                .map(StringBuilder::toString)
                .collect(Collectors.toList());
    }

    void processCreateUpdateRule(Connection conn, ForeignKey fk, LinkedList<StringBuilder> sqlQueue)  {
        StringBuilder sql = sqlQueue.peek();
        switch (fk.getUpdateRule()) {
            case SET_NULL:
                sql.append(" on update set null");
                break;
            case CASCADE:
                sql.append(" on update cascade");
                break;
            case NO_ACTION:
            default:
                break;
        }
    }

    final String createView(View v)  {
        try {
            SQLGenerator gen = getViewSQLGenerator();
            StringWriter sw = new StringWriter();
            PrintWriter bw = new PrintWriter(sw);

            v.createViewScript(bw, gen);
            bw.flush();

            String sql = sw.toString();
            return sql;
        } catch (IOException e) {
            throw new CelestaException(e);
        }
    }

    /**
     * This method is called after a table creation.
     *
     * @param t  table
     * @return  list of SQLs to be processed after a table creation
     */
    List<String> afterCreateTable(Connection conn, TableElement t) {
        return Collections.emptyList();
    }

    final String dropTable(TableElement t) {
        String sql = String.format(
                "DROP TABLE %s",
                tableString(t.getGrain().getName(), t.getName())
        );

        this.triggers.computeIfAbsent(t.getGrain().getName(), s -> new HashMap<>())
                .remove(t.getName());

        return sql;
    }

    final List<String> initDataForMaterializedView(MaterializedView mv) {
        BasicTable t = mv.getRefTable().getTable();

        String mvIdentifier = tableString(mv.getGrain().getName(), mv.getName());
        String mvColumns = mv.getColumns().keySet().stream()
                .filter(alias -> !MaterializedView.SURROGATE_COUNT.equals(alias))
                .map(alias -> "\"" + alias + "\"")
                .collect(Collectors.joining(", "))
                .concat(", \"").concat(MaterializedView.SURROGATE_COUNT).concat("\"");

        String tableGroupByColumns = mv.getColumns().values().stream()
                .filter(v -> mv.isGroupByColumn(v.getName()))
                .map(v -> {
                    Column<?> colRef = mv.getColumnRef(v.getName());
                    String groupByColStr = "\"" + mv.getColumnRef(v.getName()).getName() + "\"";

                    if (DateTimeColumn.CELESTA_TYPE.equals(colRef.getCelestaType())) {
                        return truncDate(groupByColStr);
                    }
                    return groupByColStr;
                })
                .collect(Collectors.joining(", "));

        String deleteSql = this.truncateTable(mvIdentifier);

        String colsToSelect = mv.getColumns().keySet().stream()
                .filter(alias -> !MaterializedView.SURROGATE_COUNT.equals(alias))
                .map(alias -> {
                    Column<?> colRef = mv.getColumnRef(alias);
                    Map<String, Expr> aggrCols = mv.getAggregateColumns();

                    if (aggrCols.containsKey(alias)) {
                        Expr agrExpr = aggrCols.get(alias);
                        if (agrExpr instanceof Count) {
                            return "COUNT(*)";
                        } else if (agrExpr instanceof Sum) {
                            return "SUM(\"" + colRef.getName() + "\")";
                        } else {
                            throw new RuntimeException(
                                    String.format(
                                            "Aggregate func of type %s is not supported",
                                            agrExpr.getClass().getSimpleName()
                                    )
                            );
                        }
                    } else {
                        if (DateTimeColumn.CELESTA_TYPE.equals(colRef.getCelestaType())) {
                            return truncDate("\"" + colRef.getName() + "\"");
                        }
                        return "\"" + colRef.getName() + "\"";

                    }
                }).collect(Collectors.joining(", "));

        String selectScript = String.format("SELECT " + colsToSelect + ", COUNT(*)"
                        + " FROM " + tableString(t.getGrain().getName(), t.getName()) + " GROUP BY %s",
                tableGroupByColumns);
        String insertSql = String.format("INSERT INTO %s (%s) " + selectScript, mvIdentifier, mvColumns);

        return Arrays.asList(deleteSql, insertSql);
    }

    String truncateTable(String tableName) {
        return "TRUNCATE TABLE " + tableName;
    }

    final boolean triggerExists(Connection conn, TriggerQuery query)  {
        try {
            return isTriggerKnown(query) || this.dmlAdaptor.triggerExists(conn, query);
        } catch (SQLException e) {
            throw new CelestaException(e);
        }
    }

    final void rememberTrigger(TriggerQuery query) {
        this.triggers.computeIfAbsent(query.getSchema(), s -> new HashMap<>())
                .computeIfAbsent(query.getTableName(), t -> new HashSet<>())
                .add(query.getName());
    }

    final void forgetTrigger(TriggerQuery query) {
        this.triggers.computeIfAbsent(query.getSchema(), s -> new HashMap<>())
                .computeIfAbsent(query.getTableName(), t -> new HashSet<>())
                .remove(query.getName());
    }

    final boolean isTriggerKnown(TriggerQuery query) {
        return this.triggers.computeIfAbsent(query.getSchema(), s -> new HashMap<>())
                .computeIfAbsent(query.getTableName(), t -> new HashSet<>())
                .contains(query.getName());
    }

    /**
     * Returns a translator from CelestaSQL language to the language of desired DB dialect.
     *
     * @return
     */
    abstract SQLGenerator getViewSQLGenerator();

    abstract List<String> createParameterizedView(ParameterizedView pv);

    abstract Optional<String> dropAutoIncrement(Connection conn, TableElement t);

    public abstract List<String> dropTableTriggersForMaterializedViews(Connection conn, BasicTable t);

    public abstract List<String> createTableTriggersForMaterializedViews(BasicTable t);

    /**
     * Returns an SQL with the rounding function of timestamp to date.
     *
     * @param dateStr  value that has to be rounded
     * @return
     */
    abstract String truncDate(String dateStr);
}

package ru.curs.celesta.dbutils.adaptors.ddl;


import ru.curs.celesta.CelestaException;
import ru.curs.celesta.DBType;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.dbutils.adaptors.column.ColumnDefinerFactory;
import static ru.curs.celesta.dbutils.adaptors.function.CommonFunctions.*;
import static ru.curs.celesta.dbutils.adaptors.constants.CommonConstants.*;

import ru.curs.celesta.dbutils.meta.DbColumnInfo;
import ru.curs.celesta.dbutils.meta.DbIndexInfo;
import ru.curs.celesta.event.TriggerQuery;
import ru.curs.celesta.event.TriggerType;
import ru.curs.celesta.score.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.util.*;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for SQL generation of data definition of MsSql.
 */
public final class MsSqlDdlGenerator extends DdlGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(MsSqlDdlGenerator.class);

    public MsSqlDdlGenerator(DBAdaptor dmlAdaptor) {
        super(dmlAdaptor);
    }

    @Override
    List<String> dropParameterizedView(String schemaName, String viewName, Connection conn)  {
        String sql = String.format("DROP FUNCTION %s", tableString(schemaName, viewName));
        return Arrays.asList(sql);
    }

    @Override
    List<String> dropIndex(Grain g, DbIndexInfo dBIndexInfo) {
        String sql = String.format(
                "DROP INDEX %s ON %s",
                dBIndexInfo.getIndexName(),
                tableString(g.getName(), dBIndexInfo.getTableName())
        );

        return Arrays.asList(sql);
    }

    @Override
    String dropTriggerSql(TriggerQuery query) {
        String sql = String.format(
                "drop trigger %s",
                tableString(query.getSchema(), query.getName())
        );
        return sql;
    }

    @Override
    DBType getType() {
        return DBType.MSSQL;
    }

    @Override
    List<String> updateVersioningTrigger(Connection conn, TableElement t)  {
        List<String> result = new ArrayList<>();
        // First of all, we are about to check if trigger exists
        try {
            TriggerQuery query = new TriggerQuery()
                    .withSchema(t.getGrain().getName())
                    .withTableName(t.getName())
                    .withName(t.getName() + "_upd");
            boolean triggerExists = this.triggerExists(conn, query);

            if (t instanceof VersionedElement) {
                VersionedElement ve = (VersionedElement) t;
                if (ve.isVersioned()) {
                    if (!triggerExists) {
                        result.add(createVersioningTrigger(t));
                        this.rememberTrigger(query);
                    }
                } else {
                    if (triggerExists) {
                        // DROP TRIGGER
                        result.add(dropTrigger(query));
                    }
                }
            }

        } catch (CelestaException e) {
            throw new CelestaException("Could not update version check trigger on %s.%s: %s", t.getGrain().getName(),
                    t.getName(), e.getMessage());
        }

        return result;
    }

    private String createVersioningTrigger(TableElement t)  {
        StringBuilder sb = new StringBuilder();
        sb.append(
                String.format("create trigger \"%s\".\"%s_upd\" on \"%s\".\"%s\" for update as begin%n",
                        t.getGrain().getName(), t.getName(), t.getGrain().getName(), t.getName()));
        sb.append(generateTsqlForVersioningTrigger(t));
        sb.append("end\n");
        // CREATE TRIGGER
        LOGGER.trace("{}", sb);
        return sb.toString();
    }

    @Override
    public String dropPk(TableElement t, String pkName) {
        String sql = String.format("alter table %s.%s drop constraint \"%s\"", t.getGrain().getQuotedName(),
                t.getQuotedName(), pkName);
        return sql;
    }

    private String generateTsqlForVersioningTrigger(TableElement t) {
        StringBuilder sb = new StringBuilder();
        sb.append("IF  exists (select * from inserted inner join deleted on \n");
        addPKJoin(sb, "inserted", "deleted", t);
        sb.append("where inserted.recversion <> deleted.recversion) BEGIN\n");
        sb.append("  RAISERROR ('record version check failure', 16, 1);\n");

        sb.append("END\n");
        sb.append(String.format("update \"%s\".\"%s\" set recversion = recversion + 1 where%n",
                t.getGrain().getName(), t.getName()));
        sb.append("exists (select * from inserted where \n");

        addPKJoin(sb, "inserted", String.format("\"%s\".\"%s\"", t.getGrain().getName(), t.getName()),
                t);
        sb.append(");\n");

        return sb.toString();
    }

    //TODO:Must be defined in single place
    private void addPKJoin(StringBuilder sb, String left, String right, TableElement t) {
        boolean needAnd = false;
        for (String s : t.getPrimaryKey().keySet()) {
            if (needAnd) {
                sb.append(" AND ");
            }
            sb.append(String.format("  %s.\"%s\" = %s.\"%s\"%n", left, s, right, s));
            needAnd = true;
        }
    }

    @Override
    List<String> updateColumn(Connection conn, Column<?> c, DbColumnInfo actual) {
        @SuppressWarnings("unchecked")
        final Class<? extends Column<?>> cClass = (Class<Column<?>>) c.getClass();

        List<String> result = new ArrayList<>();
        String sql;
        if (!"".equals(actual.getDefaultValue())) {
            sql = String.format(
                    ALTER_TABLE + tableString(c.getParentTable().getGrain().getName(), c.getParentTable().getName())
                            + " drop constraint \"def_%s_%s\"", c.getParentTable().getName(), c.getName());
            result.add(sql);
        }

        String def = ColumnDefinerFactory.getColumnDefiner(getType(), cClass).getMainDefinition(c);
        sql = String.format(ALTER_TABLE + tableString(c.getParentTable().getGrain().getName(),
                c.getParentTable().getName()) + " alter column %s", def);
        result.add(sql);

        def = ColumnDefinerFactory.getColumnDefiner(getType(), cClass).getDefaultDefinition(c);
        if (!"".equals(def)) {
            sql = String.format(ALTER_TABLE + tableString(c.getParentTable().getGrain().getName(),
                    c.getParentTable().getName()) + " add %s for %s",
                    def, c.getQuotedName());
            result.add(sql);
        }

        return result;
    }

    @Override
    List<String> createIndex(Index index) {
        String fieldList = getFieldList(index.getColumns().keySet());
        String sql = String.format("CREATE INDEX %s ON "
                + tableString(index.getTable().getGrain().getName(), index.getTable().getName())
                + " (%s)", index.getQuotedName(), fieldList);
        return Arrays.asList(sql);
    }

    @Override
    public SQLGenerator getViewSQLGenerator() {
        return new SQLGenerator() {

            @Override
            protected String concat() {
                return " + ";
            }

            @Override
            protected String preamble(AbstractView view) {
                return String.format("create view %s as", viewName(view));
            }

            @Override
            protected String boolLiteral(boolean val) {
                return val ? "1" : "0";
            }

            @Override
            protected String paramLiteral(String paramName) {
                return "@" + paramName;
            }
        };
    }

    @Override
    List<String> createParameterizedView(ParameterizedView pv)  {
        SQLGenerator gen = getViewSQLGenerator();
        StringWriter sw = new StringWriter();
        PrintWriter bw = new PrintWriter(sw);

        try {
            pv.selectScript(bw, gen);
        } catch (IOException e) {
            throw new CelestaException(e);
        }
        bw.flush();

        String inParams = pv.getParameters()
                .entrySet().stream()
                .map(e ->
                        "@" + e.getKey() + " "
                                + ColumnDefinerFactory.getColumnDefiner(getType(),
                                CELESTA_TYPES_COLUMN_CLASSES.get(e.getValue().getType().getCelestaType())
                        ).dbFieldType()
                ).collect(Collectors.joining(", "));


        String selectSql = sw.toString();

        String sql = String.format(
                "CREATE FUNCTION " + tableString(pv.getGrain().getName(), pv.getName()) + "(%s)%n"
                        + "  RETURNS TABLE%n"
                        + "  AS%n"
                        + "  RETURN %s",
                                   inParams, selectSql);

        return Arrays.asList(sql);
    }

    @Override
    Optional<String> dropAutoIncrement(Connection conn, TableElement t) {
        String sql = String.format("delete from " + t.getGrain().getScore().getSysSchemaName()
                        + ".sequences where grainid = '%s' and tablename = '%s';%n",
                t.getGrain().getName(), t.getName());
        return Optional.of(sql);
    }

    @Override
    String truncDate(String dateStr) {
        return "cast(floor(cast(" + dateStr + " as float)) as datetime)";
    }

    @Override
    public List<String> dropTableTriggersForMaterializedViews(Connection conn, BasicTable t)  {
        List<String> result = new ArrayList<>();

        List<MaterializedView> mvList = t.getGrain().getElements(MaterializedView.class).values().stream()
                .filter(mv -> mv.getRefTable().getTable().equals(t))
                .collect(Collectors.toList());

        TriggerQuery query = new TriggerQuery()
                .withSchema(t.getGrain().getName())
                .withTableName(t.getName());

        for (MaterializedView mv : mvList) {

            String insertTriggerName = mv.getTriggerName(TriggerType.POST_INSERT);
            String deleteTriggerName = mv.getTriggerName(TriggerType.POST_DELETE);


            query.withName(insertTriggerName);
            if (this.triggerExists(conn, query)) {
                result.add(dropTrigger(query));
            }
            query.withName(deleteTriggerName);
            if (this.triggerExists(conn, query)) {
                result.add(dropTrigger(query));
            }
        }

        if (!mvList.isEmpty()) {
            // Set excessive 'rec_version' trigger to zero.
            query.withName(t.getName() + "_upd");
            if (this.triggerExists(conn, query)) {
                result.add(dropTrigger(query));
            }
            if (t instanceof VersionedElement && ((VersionedElement) t).isVersioned()) {
                result.add(createVersioningTrigger(t));
                this.rememberTrigger(query);
            }
        }

        return result;
    }

    @Override
    public List<String> createTableTriggersForMaterializedViews(BasicTable t) {
        List<String> result = new ArrayList<>();

        String fullTableName = tableString(t.getGrain().getName(), t.getName());

        List<MaterializedView> mvList = t.getGrain().getElements(MaterializedView.class).values().stream()
                .filter(mv -> mv.getRefTable().getTable().equals(t))
                .collect(Collectors.toList());

        if (mvList.isEmpty()) {
            return result;
        }

        StringBuilder afterUpdateTriggerTsql = new StringBuilder();

        if (t instanceof VersionedElement && ((VersionedElement) t).isVersioned()) {
            afterUpdateTriggerTsql.append(generateTsqlForVersioningTrigger(t)).append("\n");
        }

        TriggerQuery query = new TriggerQuery()
                .withSchema(t.getGrain().getName())
                .withTableName(t.getName());

        for (MaterializedView mv : mvList) {
            String fullMvName = tableString(mv.getGrain().getName(), mv.getName());

            String insertTriggerName = mv.getTriggerName(TriggerType.POST_INSERT);
            String deleteTriggerName = mv.getTriggerName(TriggerType.POST_DELETE);

            String mvColumns = mv.getColumns().keySet().stream()
                    .filter(alias -> !MaterializedView.SURROGATE_COUNT.equals(alias))
                    .collect(Collectors.joining(", "))
                    .concat(", " + MaterializedView.SURROGATE_COUNT);

            String aggregateColumns = mv.getColumns().keySet().stream()
                    .filter(alias -> !MaterializedView.SURROGATE_COUNT.equals(alias))
                    .map(alias -> "aggregate." + alias)
                    .collect(Collectors.joining(", "))
                    .concat(", " + MaterializedView.SURROGATE_COUNT);


            String rowConditionTemplate = mv.getColumns().keySet().stream()
                    .filter(alias -> mv.isGroupByColumn(alias))
                    .map(alias -> "mv." + alias + " = %1$s." + alias + " ")
                    .collect(Collectors.joining(" AND "));

            String rowConditionForExistsTemplate = mv.getColumns().keySet().stream()
                    .filter(alias -> mv.isGroupByColumn(alias))
                    .map(alias -> {
                        Column<?> colRef = mv.getColumnRef(alias);

                        if (DateTimeColumn.CELESTA_TYPE.equals(colRef.getCelestaType())) {
                            return "mv." + alias + " = cast(floor(cast(%1$s."
                                         + mv.getColumnRef(alias).getName() + " as float)) as datetime)";
                        }

                        return "mv." + alias + " = %1$s." + mv.getColumnRef(alias).getName() + " ";
                    })
                    .collect(Collectors.joining(" AND "));

            String setStatementTemplate = mv.getAggregateColumns().entrySet().stream()
                    .map(e -> {
                        StringBuilder sb = new StringBuilder();
                        String alias = e.getKey();

                        sb.append("mv.").append(alias)
                                .append(" = mv.").append(alias)
                                .append(" %1$s aggregate.").append(alias);

                        return sb.toString();
                    }).collect(Collectors.joining(", "))
                    .concat(", mv.").concat(MaterializedView.SURROGATE_COUNT).concat(" = ")
                    .concat("mv.").concat(MaterializedView.SURROGATE_COUNT).concat(" %1$s aggregate.")
                    .concat(MaterializedView.SURROGATE_COUNT);

            String tableGroupByColumns = mv.getColumns().values().stream()
                    .filter(v -> mv.isGroupByColumn(v.getName()))
                    .map(v -> {
                                if (DateTimeColumn.CELESTA_TYPE.equals(v.getCelestaType())) {
                                    return "cast(floor(cast(\"" + v.getName() + "\" as float)) as datetime)";
                                }
                                return "\"" + mv.getColumnRef(v.getName()).getName() + "\"";
                            }
                    ).collect(Collectors.joining(", "));


            String selectPartOfScript = mv.getColumns().keySet().stream()
                    .filter(alias -> !MaterializedView.SURROGATE_COUNT.equals(alias))
                    .map(alias -> {
                        Column<?> colRef = mv.getColumnRef(alias);

                        Map<String, Expr> aggrCols = mv.getAggregateColumns();
                        if (aggrCols.containsKey(alias)) {
                            if (colRef == null) {
                                if (aggrCols.get(alias) instanceof Count) {
                                    return "COUNT(*) as \"" + alias + "\"";
                                }
                                return "";
                            } else if (aggrCols.get(alias) instanceof Sum) {
                                return "SUM(\"" + colRef.getName() + "\") as \"" + alias + "\"";
                            } else {
                                return "";
                            }
                        }

                        if (DateTimeColumn.CELESTA_TYPE.equals(colRef.getCelestaType())) {
                            return "cast(floor(cast(\"" + colRef.getName() + "\" as float)) as datetime) "
                                    + "as \"" + alias + "\"";
                        }

                        return "\"" + colRef.getName() + "\" as " + "\"" + alias + "\"";
                    })
                    .filter(str -> !str.isEmpty())
                    .collect(Collectors.joining(", "))
                    .concat(", COUNT(*) AS " + MaterializedView.SURROGATE_COUNT);

            StringBuilder insertSqlBuilder = new StringBuilder("MERGE INTO %s WITH (HOLDLOCK) AS mv \n")
                    .append("USING (SELECT %s FROM inserted GROUP BY %s) AS aggregate ON %s \n")
                    .append("WHEN MATCHED THEN \n ")
                    .append("UPDATE SET %s \n")
                    .append("WHEN NOT MATCHED THEN \n")
                    .append("INSERT (%s) VALUES (%s); \n");

            String insertSql = String.format(insertSqlBuilder.toString(), fullMvName,
                    selectPartOfScript, tableGroupByColumns, String.format(rowConditionTemplate, "aggregate"),
                    String.format(setStatementTemplate, "+"), mvColumns, aggregateColumns);

            String deleteMatchedCondTemplate = mv.getAggregateColumns().keySet().stream()
                    .map(alias -> "mv." + alias + " %1$s aggregate." + alias)
                    .collect(Collectors.joining(" %2$s "));

            String existsSql = "EXISTS(SELECT * FROM " + fullTableName + " AS t WHERE "
                    + String.format(rowConditionForExistsTemplate, "t") + ")";

            StringBuilder deleteSqlBuilder = new StringBuilder("MERGE INTO %s WITH (HOLDLOCK) AS mv \n")
                    .append("USING (SELECT %s FROM deleted GROUP BY %s) AS aggregate ON %s \n")
                    .append("WHEN MATCHED AND %s THEN DELETE\n ")
                    .append("WHEN MATCHED AND (%s) THEN \n")
                    .append("UPDATE SET %s; \n");

            String deleteSql = String.format(deleteSqlBuilder.toString(), fullMvName,
                    selectPartOfScript, tableGroupByColumns, String.format(rowConditionTemplate, "aggregate"),
                    String.format(deleteMatchedCondTemplate, "=", "AND").concat(" AND NOT " + existsSql),
                    String.format(deleteMatchedCondTemplate, "<>", "OR")
                            .concat(" OR (" + String.format(deleteMatchedCondTemplate, "=", "AND")
                                    .concat(" AND " + existsSql + ")")),
                    String.format(setStatementTemplate, "-"));

            String sql;
            //INSERT

            sql = String.format("create trigger \"%s\".\"%s\" "
                            + "on %s after insert as begin %n"
                            + MaterializedView.CHECKSUM_COMMENT_TEMPLATE
                            + "%n %s %n END;",
                    t.getGrain().getName(), insertTriggerName, fullTableName, mv.getChecksum(), insertSql);
            LOGGER.trace(sql);
            result.add(sql);
            this.rememberTrigger(query.withName(insertTriggerName));

            //UPDATE
            //Инструкции для update-триггера нужно собирать и использовать после прогона главного цикла метода
            afterUpdateTriggerTsql.append(String.format("%n%s%n %n%s%n", deleteSql, insertSql));
            //DELETE

            sql = String.format("create trigger \"%s\".\"%s\" on %s after delete as begin %n %s %n END;",
                                t.getGrain().getName(), deleteTriggerName, fullTableName, deleteSql);
            LOGGER.trace(sql);
            result.add(sql);
            this.rememberTrigger(query.withName(deleteTriggerName));

        }


        StringBuilder sb = new StringBuilder();

        final String sqlPrefix =
                (t instanceof VersionedElement && ((VersionedElement) t).isVersioned()) ? "alter" : "create";

        String updateTriggerName = String.format("%s_upd", t.getName());
        sb.append(
                String.format("%s trigger \"%s\".\"%s\" on \"%s\".\"%s\" for update as begin%n",
                        sqlPrefix, t.getGrain().getName(), updateTriggerName, t.getGrain().getName(), t.getName()));
        sb.append(afterUpdateTriggerTsql.toString());
        sb.append("end\n");

        result.add(sb.toString());
        this.rememberTrigger(query.withName(updateTriggerName));

        return result;
    }

}

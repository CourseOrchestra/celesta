package ru.curs.celesta.dbutils.adaptors.ddl;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.DBType;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.dbutils.adaptors.column.ColumnDefinerFactory;
import ru.curs.celesta.dbutils.jdbc.SqlUtils;
import ru.curs.celesta.dbutils.meta.DbColumnInfo;
import ru.curs.celesta.event.TriggerQuery;
import ru.curs.celesta.event.TriggerType;
import ru.curs.celesta.score.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static ru.curs.celesta.dbutils.adaptors.constants.CommonConstants.*;
import static ru.curs.celesta.dbutils.adaptors.constants.OpenSourceConstants.*;

/**
 * Class for SQL generation of data definition of PostgreSQL.
 */
public final class PostgresDdlGenerator extends OpenSourceDdlGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresDdlGenerator.class);

    public PostgresDdlGenerator(DBAdaptor dmlAdaptor) {
        super(dmlAdaptor);
    }

    @Override
    List<String> dropParameterizedView(String schemaName, String viewName, Connection conn) {
        List<String> result = new ArrayList<>();

        String sql = "SELECT format('DROP FUNCTION IF EXISTS %s(%s);',\n"
                + "  p.oid::regproc, pg_get_function_identity_arguments(p.oid))\n"
                + " FROM pg_catalog.pg_proc p\n"
                + " LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace\n"
                + " WHERE\n"
                + " p.oid::regproc::text = '" + String.format("%s.%s", schemaName, viewName) + "';";

        try (ResultSet rs = SqlUtils.executeQuery(conn, sql)) {
            if (rs.next()) {
                String dropSql = rs.getString(1);
                result.add(dropSql);
            }
        } catch (SQLException e) {
            throw new CelestaException(e);
        }

        return result;
    }

    @Override
    DBType getType() {
        return DBType.POSTGRESQL;
    }

    @Override
    List<String> updateVersioningTrigger(Connection conn, TableElement t) {
        List<String> result = new ArrayList<>();
        // First of all, we are about to check if trigger exists
        try {
            TriggerQuery query = new TriggerQuery().withSchema(t.getGrain().getName())
                    .withName("versioncheck")
                    .withTableName(t.getName());
            boolean triggerExists = this.triggerExists(conn, query);

            if (t instanceof VersionedElement) {
                VersionedElement ve = (VersionedElement) t;

                String sql;
                if (ve.isVersioned()) {
                    if (!triggerExists) {
                        // CREATE TRIGGER
                        sql =
                                "CREATE TRIGGER \"versioncheck\""
                                        + " BEFORE UPDATE ON " + tableString(t.getGrain().getName(), t.getName())
                                        + " FOR EACH ROW EXECUTE PROCEDURE "
                                        + t.getGrain().getScore().getSysSchemaName() + ".recversion_check();";
                        result.add(sql);
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

    @Override
    public String dropPk(TableElement t, String pkName) {
        String sql = String.format("alter table %s.%s drop constraint \"%s\" cascade", t.getGrain().getQuotedName(),
                t.getQuotedName(), pkName);
        return sql;
    }

    @Override
    void updateColType(Column<?> c, DbColumnInfo actual, List<String> sqlList) {
        @SuppressWarnings("unchecked")
        final Class<? extends Column<?>> cClass = (Class<Column<?>>) c.getClass();
        String colType;
        if (c.getClass() == StringColumn.class) {
            StringColumn sc = (StringColumn) c;
            colType = sc.isMax() ? "text" : String.format(
                    "%s(%s)",
                    ColumnDefinerFactory.getColumnDefiner(getType(), cClass).dbFieldType(), sc.getLength()
            );
        } else if (c.getClass() == DecimalColumn.class) {
            DecimalColumn dc = (DecimalColumn) c;
            colType = String.format(
                    "%s(%s,%s)",
                    ColumnDefinerFactory.getColumnDefiner(getType(), cClass).dbFieldType(),
                    dc.getPrecision(), dc.getScale()
            );
        } else {
            colType = ColumnDefinerFactory.getColumnDefiner(getType(), cClass).dbFieldType();
        }

        StringBuilder alterSql = new StringBuilder(
                String.format(
                        ALTER_TABLE + tableString(c.getParentTable().getGrain().getName(), c.getParentTable().getName())
                                + " ALTER COLUMN \"%s\" TYPE %s", c.getName(), colType
                )
        );

        // If type doesn't match
        if (c.getClass() != actual.getType()) {
            if (c.getClass() == IntegerColumn.class) {
                alterSql.append(String.format(" USING (%s::integer);", c.getQuotedName()));
            } else if (c.getClass() == BooleanColumn.class) {
                alterSql.append(String.format(" USING (%s::boolean);", c.getQuotedName()));
            }

            sqlList.add(alterSql.toString());
        } else if (c.getClass() == StringColumn.class) {
            StringColumn sc = (StringColumn) c;
            if (sc.isMax() != actual.isMax() || sc.getLength() != actual.getLength()) {
                sqlList.add(alterSql.toString());
            }
        } else if (c.getClass() == DecimalColumn.class) {
            DecimalColumn dc = (DecimalColumn) c;
            if (dc.getPrecision() != actual.getLength() || dc.getScale() != dc.getScale()) {
                sqlList.add(alterSql.toString());
            }
        }
    }

    @Override
    List<String> createIndex(Index index) {
        List<String> result = new ArrayList<>();

        StringBuilder sb = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        boolean conjugate = false;
        for (Map.Entry<String, Column<?>> c : index.getColumns().entrySet()) {
            if (sb.length() > 0) {
                sb.append(", ");
                sb2.append(", ");
            }
            sb.append('"');
            sb2.append('"');
            sb.append(c.getKey());
            sb2.append(c.getKey());
            sb.append('"');
            sb2.append('"');

            if (c.getValue() instanceof StringColumn && !((StringColumn) c.getValue()).isMax()) {
                sb2.append(" varchar_pattern_ops");
                conjugate = true;
            }
        }

        String sql = String.format(
                "CREATE INDEX \"%s\" ON "
                        + tableString(index.getTable().getGrain().getName(), index.getTable().getName())
                        + " (%s)", index.getName(), sb.toString());
        result.add(sql);
        if (conjugate) {
            sql = String.format(
                    "CREATE INDEX \"%s\" ON "
                            + tableString(index.getTable().getGrain().getName(), index.getTable().getName())
                            + " (%s)", index.getName() + CONJUGATE_INDEX_POSTFIX, sb2.toString());
            result.add(sql);
        }

        return result;
    }

    @Override
    public SQLGenerator getViewSQLGenerator() {
        return new SQLGenerator() {
            @Override
            protected String paramLiteral(String paramName) {
                return paramName;
            }

            @Override
            protected String getDate() {
                return "CURRENT_TIMESTAMP";
            }
        };
    }

    @Override
    List<String> createParameterizedView(ParameterizedView pv) {
        SQLGenerator gen = getViewSQLGenerator();
        StringWriter sw = new StringWriter();
        PrintWriter bw = new PrintWriter(sw);

        try {
            pv.selectScript(bw, gen);
        } catch (IOException e) {
            throw new CelestaException(e);
        }
        bw.flush();

        String pvParams = pv.getParameters()
                .entrySet().stream()
                .map(e ->
                        e.getKey() + " "
                                + ColumnDefinerFactory.getColumnDefiner(getType(),
                                CELESTA_TYPES_COLUMN_CLASSES.get(e.getValue().getType().getCelestaType())
                        ).dbFieldType()

                ).collect(Collectors.joining(", "));

        String pViewCols = pv.getColumns().entrySet().stream()
                .map(e -> {
                            StringBuilder sb = new StringBuilder("\"")
                                    .append(e.getKey()).append("\" ");

                            if (pv.getAggregateColumns().containsKey(e.getKey())
                                    && e.getValue().getColumnType() != ViewColumnType.DECIMAL) {
                                sb.append("bigint");
                            } else {
                                sb.append(ColumnDefinerFactory.getColumnDefiner(getType(),
                                        CELESTA_TYPES_COLUMN_CLASSES.get(e.getValue().getCelestaType()))
                                        .dbFieldType());
                            }

                            return sb.toString();
                        }
                ).collect(Collectors.joining(", "));

        String selectSql = sw.toString();


        String sql = String.format(
                "create or replace function " + tableString(pv.getGrain().getName(), pv.getName())
                        + "(%s) returns TABLE(%s) AS\n"
                        + "$$\n %s $$\n"
                        + "language sql;", pvParams, pViewCols, selectSql);

        return Arrays.asList(sql);
    }

    @Override
    String truncDate(String dateStr) {
        return "date_trunc('DAY'," + dateStr + ")";
    }

    @Override
    public List<String> dropTableTriggersForMaterializedViews(Connection conn, BasicTable t) {
        List<String> result = new ArrayList<>();

        List<MaterializedView> mvList = t.getGrain().getElements(MaterializedView.class).values().stream()
                .filter(mv -> mv.getRefTable().getTable().equals(t))
                .collect(Collectors.toList());

        for (MaterializedView mv : mvList) {

            TriggerQuery query = new TriggerQuery()
                    .withSchema(t.getGrain().getName())
                    .withTableName(t.getName());

            String insertTriggerName = mv.getTriggerName(TriggerType.POST_INSERT);
            String updateTriggerName = mv.getTriggerName(TriggerType.POST_UPDATE);
            String deleteTriggerName = mv.getTriggerName(TriggerType.POST_DELETE);

            String insertTriggerFunctionFullName = String.format("\"%s\".\"%s_insertTriggerFunc\"()",
                    t.getGrain().getName(), mv.getName());
            String updateTriggerFunctionFullName = String.format("\"%s\".\"%s_updateTriggerFunc\"()",
                    t.getGrain().getName(), mv.getName());
            String deleteTriggerFunctionFullName = String.format("\"%s\".\"%s_deleteTriggerFunc\"()",
                    t.getGrain().getName(), mv.getName());


            query.withName(insertTriggerName);
            if (this.triggerExists(conn, query)) {
                result.add(dropTrigger(query));
            }
            query.withName(updateTriggerName);
            if (this.triggerExists(conn, query)) {
                result.add(dropTrigger(query));
            }
            query.withName(deleteTriggerName);
            if (this.triggerExists(conn, query)) {
                result.add(dropTrigger(query));
            }

            String sqlTemplate = "DROP FUNCTION IF EXISTS %s";

            String sql;

            //INSERT
            sql = String.format(sqlTemplate, insertTriggerFunctionFullName);
            result.add(sql);
            //UPDATE
            sql = String.format(sqlTemplate, updateTriggerFunctionFullName);
            result.add(sql);
            //DELETE
            sql = String.format(sqlTemplate, deleteTriggerFunctionFullName);
            result.add(sql);

        }

        return result;
    }

    @Override
    public List<String> createTableTriggersForMaterializedViews(BasicTable t) {
        List<String> result = new ArrayList<>();

        List<MaterializedView> mvList = t.getGrain().getElements(MaterializedView.class).values().stream()
                .filter(mv -> mv.getRefTable().getTable().equals(t))
                .collect(Collectors.toList());

        String fullTableName = tableString(t.getGrain().getName(), t.getName());

        TriggerQuery query = new TriggerQuery()
                .withSchema(t.getGrain().getName())
                .withTableName(t.getName());

        for (MaterializedView mv : mvList) {
            String fullMvName = tableString(mv.getGrain().getName(), mv.getName());

            String insertTriggerName = mv.getTriggerName(TriggerType.POST_INSERT);
            String updateTriggerName = mv.getTriggerName(TriggerType.POST_UPDATE);
            String deleteTriggerName = mv.getTriggerName(TriggerType.POST_DELETE);

            //functions are unique for postgres
            String insertTriggerFunctionFullName = String.format("\"%s\".\"%s_insertTriggerFunc\"()",
                    t.getGrain().getName(), mv.getName());
            String updateTriggerFunctionFullName = String.format("\"%s\".\"%s_updateTriggerFunc\"()",
                    t.getGrain().getName(), mv.getName());
            String deleteTriggerFunctionFullName = String.format("\"%s\".\"%s_deleteTriggerFunc\"()",
                    t.getGrain().getName(), mv.getName());

            String mvColumns = mv.getColumns().keySet().stream()
                    .filter(alias -> !MaterializedView.SURROGATE_COUNT.equals(alias))
                    .collect(Collectors.joining(", "));

            String whereCondition = mv.getColumns().keySet().stream()
                    .filter(alias -> mv.isGroupByColumn(alias))
                    .map(alias -> alias + " = $1." + alias + " ")
                    .collect(Collectors.joining(" AND "));

            StringBuilder selectStmtBuilder = new StringBuilder(mv.getSelectPartOfScript())
                    .append(" FROM ").append(fullTableName).append(" ");
            selectStmtBuilder.append(" WHERE ").append(whereCondition)
                    .append(mv.getGroupByPartOfScript());


            String setStatementTemplate = mv.getAggregateColumns().entrySet().stream()
                    .map(e -> {
                        StringBuilder sb = new StringBuilder();
                        String alias = e.getKey();

                        sb.append("\"").append(alias.replace("\"", ""))
                                .append("\" = \"").append(alias.replace("\"", ""))
                                .append("\" %1$s ");

                        if (e.getValue() instanceof Sum) {
                            sb.append("%2$s.\"")
                                    .append(mv.getColumnRef(alias.replace("\"", "")).getName())
                                    .append("\"");
                        } else if (e.getValue() instanceof Count) {
                            sb.append("1");
                        }

                        return sb.toString();
                    }).collect(Collectors.joining(", "))
                    .concat(", \"").concat(MaterializedView.SURROGATE_COUNT).concat("\" = ")
                    .concat("\"").concat(MaterializedView.SURROGATE_COUNT).concat("\" %1$s 1");

            String rowConditionTemplate = mv.getColumns().keySet().stream()
                    .filter(alias -> mv.isGroupByColumn(alias))
                    .map(alias -> {
                                Column<?> colRef = mv.getColumnRef(alias);
                                if (DateTimeColumn.CELESTA_TYPE.equals(colRef.getCelestaType())) {
                                    return "\"" + alias + "\" = date_trunc('DAY', %1$s.\"" + colRef.getName() + "\")";
                                }
                                return "\"" + alias + "\" = %1$s.\"" + colRef.getName() + "\"";
                            }
                    ).collect(Collectors.joining(" AND "));

            String rowColumnsTemplate = mv.getColumns().keySet().stream()
                    .filter(alias -> !MaterializedView.SURROGATE_COUNT.equals(alias))
                    .map(alias -> {
                        Map<String, Expr> aggrCols = mv.getAggregateColumns();

                        if (aggrCols.containsKey(alias) && aggrCols.get(alias) instanceof Count) {
                            return "1";
                        } else {
                            Column<?> colRef = mv.getColumnRef(alias);

                            if (DateTimeColumn.CELESTA_TYPE.equals(colRef.getCelestaType())) {
                                return "date_trunc('DAY', %1$s.\"" + mv.getColumnRef(alias) + "\")";
                            }
                            return "%1$s.\"" + mv.getColumnRef(alias) + "\"";
                        }
                    })
                    .collect(Collectors.joining(", "));

            String whereForDelete = new StringBuilder().append(String.format(rowConditionTemplate, "OLD"))
                    .append(" AND \"" + MaterializedView.SURROGATE_COUNT + "\" = 0 ")
                    .toString();

            String insertSql = String.format(
                    "UPDATE %s SET %s WHERE %s ;\n"
                            + "GET DIAGNOSTICS updatedCount = ROW_COUNT; \n"
                            + "IF updatedCount = 0 THEN \n"
                            + " INSERT INTO %s (%s) VALUES(%s); \n"
                            + "END IF;\n",
                    fullMvName, String.format(setStatementTemplate, "+", "NEW"),
                    String.format(rowConditionTemplate, "NEW"), fullMvName,
                    mvColumns + ", " + MaterializedView.SURROGATE_COUNT,
                    String.format(rowColumnsTemplate, "NEW") + ", 1");

            String deleteSql = String.format(
                    "UPDATE %s SET %s WHERE %s ;\n"
                            + "DELETE FROM %s WHERE %s ;\n",
                    fullMvName, String.format(setStatementTemplate, "-", "OLD"),
                    String.format(rowConditionTemplate, "OLD"), fullMvName, whereForDelete);

            String sql;

            //INSERT
            sql = String.format(
                    "CREATE OR REPLACE FUNCTION %s RETURNS trigger AS $BODY$ \n "
                            + "DECLARE\n"
                            + "updatedCount int;\n"
                            + "BEGIN \n"
                            + MaterializedView.CHECKSUM_COMMENT_TEMPLATE + "\n"
                            + "LOCK TABLE ONLY %s IN EXCLUSIVE MODE; \n"
                            + "%s "
                            + "RETURN NEW; END; $BODY$\n" + "  LANGUAGE plpgsql VOLATILE COST 100;",
                    insertTriggerFunctionFullName, mv.getChecksum(), fullMvName, insertSql);

            LOGGER.trace(sql);
            result.add(sql);

            sql = String.format(
                    "CREATE TRIGGER \"%s\" AFTER INSERT ON %s FOR EACH ROW EXECUTE PROCEDURE %s",
                    insertTriggerName, fullTableName, insertTriggerFunctionFullName);

            LOGGER.trace(sql);
            result.add(sql);
            this.rememberTrigger(query.withName(insertTriggerName));

            //UPDATE
            sql = String.format(
                    "CREATE OR REPLACE FUNCTION %s RETURNS trigger AS $BODY$ \n "
                            + "DECLARE\n"
                            + "updatedCount int;\n"
                            + "BEGIN \n"
                            + "LOCK TABLE ONLY %s IN EXCLUSIVE MODE; \n"
                            + "%s " //DELETE
                            + "%s " //INSERT
                            + "RETURN NEW; END; $BODY$\n" + "  LANGUAGE plpgsql VOLATILE COST 100;",
                    updateTriggerFunctionFullName, fullMvName, deleteSql, insertSql);

            LOGGER.trace(sql);
            result.add(sql);

            sql = String.format(
                    "CREATE TRIGGER \"%s\" AFTER UPDATE ON %s FOR EACH ROW EXECUTE PROCEDURE %s",
                    updateTriggerName, fullTableName, updateTriggerFunctionFullName);

            LOGGER.trace(sql);
            result.add(sql);
            this.rememberTrigger(query.withName(updateTriggerName));

            //DELETE
            sql = String.format(
                    "CREATE OR REPLACE FUNCTION %s RETURNS trigger AS $BODY$ \n "
                            + "BEGIN \n"
                            + "LOCK TABLE ONLY %s IN EXCLUSIVE MODE; \n"
                            + "%s"
                            + "RETURN OLD; END; $BODY$\n" + "  LANGUAGE plpgsql VOLATILE COST 100;",
                    deleteTriggerFunctionFullName, fullMvName, deleteSql
            );

            LOGGER.trace(sql);
            result.add(sql);

            sql = String.format(
                    "CREATE TRIGGER \"%s\" AFTER DELETE ON %s FOR EACH ROW EXECUTE PROCEDURE %s",
                    deleteTriggerName, fullTableName, deleteTriggerFunctionFullName);

            LOGGER.trace(sql);
            result.add(sql);
            this.rememberTrigger(query.withName(deleteTriggerName));
        }

        return result;
    }

}

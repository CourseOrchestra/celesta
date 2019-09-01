package ru.curs.celesta.dbutils.adaptors.ddl;


import ru.curs.celesta.CelestaException;
import ru.curs.celesta.DBType;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.dbutils.adaptors.column.ColumnDefinerFactory;
import ru.curs.celesta.dbutils.h2.MaterializedViewDeleteTrigger;
import ru.curs.celesta.dbutils.h2.MaterializedViewInsertTrigger;
import ru.curs.celesta.dbutils.h2.MaterializedViewUpdateTrigger;
import ru.curs.celesta.dbutils.h2.RecVersionCheckTrigger;
import static ru.curs.celesta.dbutils.adaptors.constants.CommonConstants.*;
import static ru.curs.celesta.dbutils.adaptors.function.CommonFunctions.*;

import ru.curs.celesta.dbutils.meta.DbColumnInfo;
import ru.curs.celesta.event.TriggerQuery;
import ru.curs.celesta.event.TriggerType;
import ru.curs.celesta.score.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Class for SQL generation of data definition of H2.
 */
public final class H2DdlGenerator extends OpenSourceDdlGenerator {

    public H2DdlGenerator(DBAdaptor dmlAdaptor) {
        super(dmlAdaptor);
    }

    @Override
    List<String> dropParameterizedView(String schemaName, String viewName, Connection conn)  {
        String sql = String.format("DROP ALIAS IF EXISTS %s", tableString(schemaName, viewName));
        return Arrays.asList(sql);
    }

    @Override
    DBType getType() {
        return DBType.H2;
    }

    @Override
    List<String> updateVersioningTrigger(Connection conn, TableElement t)  {
        // First of all, we are about to check if trigger exists
        List<String> result = new ArrayList<>();

        try {
            String triggerName = String.format("versioncheck_%s", t.getName());
            TriggerQuery query = new TriggerQuery().withSchema(t.getGrain().getName())
                    .withName(triggerName)
                    .withTableName(t.getName());
            boolean triggerExists = this.triggerExists(conn, query);

            if (t instanceof VersionedElement) {
                VersionedElement ve = (VersionedElement) t;

                String sql;
                if (ve.isVersioned()) {
                    if (triggerExists) {
                        return result;
                    } else {
                        // CREATE TRIGGER
                        sql = String.format(
                                "CREATE TRIGGER \"%s\"" + " BEFORE UPDATE ON "
                                        + tableString(t.getGrain().getName(), t.getName())
                                        + " FOR EACH ROW CALL \"%s\"",
                                triggerName,
                                RecVersionCheckTrigger.class.getName());
                        result.add(sql);
                        this.rememberTrigger(query);
                    }
                } else {
                    if (triggerExists) {
                        // DROP TRIGGER
                        result.add(dropTrigger(query));
                        this.forgetTrigger(query);
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
        String sql = String.format("alter table %s.%s drop primary key", t.getGrain().getQuotedName(),
                t.getQuotedName());
        return sql;
    }

    @Override
    void updateColType(Column<?> c, DbColumnInfo actual, List<String> sqlList) {
        @SuppressWarnings("unchecked")
        final Class<? extends Column<?>> cClass = (Class<Column<?>>) c.getClass();

        String colType;
        if (cClass == StringColumn.class) {
            StringColumn sc = (StringColumn) c;
            colType = sc.isMax() ? "clob" : String.format(
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


        final String alterSql = String.format(
                ALTER_TABLE + tableString(c.getParentTable().getGrain().getName(), c.getParentTable().getName())
                        + " ALTER COLUMN \"%s\" %s", c.getName(), colType
        );

        // If type doesn't match
        if (c.getClass() != actual.getType()) {
            sqlList.add(alterSql);
        } else if (c.getClass() == StringColumn.class) {
            StringColumn sc = (StringColumn) c;
            if (sc.isMax() != actual.isMax() || sc.getLength() != actual.getLength()) {
                sqlList.add(alterSql);
            }
        } else if (c.getClass() == DecimalColumn.class) {
            DecimalColumn dc = (DecimalColumn) c;
            if (dc.getPrecision() != actual.getLength() || dc.getScale() != dc.getScale()) {
                sqlList.add(alterSql);
            }
        }
    }

    @Override
    List<String> createIndex(Index index) {
        String grainName = index.getTable().getGrain().getName();
        String fieldList = getFieldList(index.getColumns().keySet());
        String sql = String.format("CREATE INDEX " + tableString(grainName, index.getName())
                + " ON " + tableString(grainName, index.getTable().getName()) + " (%s)", fieldList);
        return Arrays.asList(sql);
    }

    @Override
    public SQLGenerator getViewSQLGenerator() {
        return new SQLGenerator() {
            @Override
            protected String paramLiteral(String paramName) {
                return "?";
            }

            @Override
            protected String getDate() {
                return "CURRENT_TIMESTAMP";
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

        String selectSql = sw.toString();

        String inputParams = pv.getParameters().values().stream()
                .map(p -> p.getJavaClass().getName() + " " + p.getName())
                .collect(Collectors.joining(", "));

        List<String> paramRefsWithOrder = pv.getParameterRefsWithOrder();

        StringBuilder paramSettingBuilder = new StringBuilder();

        int settingPosition = 1;

        for (String param : paramRefsWithOrder) {
            paramSettingBuilder.append("ps.setObject(" + settingPosition + "," + param + ");");
            ++settingPosition;
        }

        selectSql = selectSql.replace("\"", "\\\"");
        selectSql = selectSql.replaceAll("\\R", "");

        String sql = String.format(
                "CREATE ALIAS " + tableString(pv.getGrain().getName(), pv.getName()) + " AS $$ "
                      + "java.sql.ResultSet %s(java.sql.Connection conn, %s) throws java.sql.SQLException {"
                          + "java.sql.PreparedStatement ps = conn.prepareStatement(\"%s\");"
                          + "%s"
                          + "return ps.executeQuery();"
                      + "} $$;",
                pv.getName(),
                inputParams, selectSql, paramSettingBuilder.toString());

        return Arrays.asList(sql);
    }

    @Override
    String truncDate(String dateStr) {
        return "TRUNC(" + dateStr + ")";
    }

    @Override
    public List<String> dropTableTriggersForMaterializedViews(Connection conn, BasicTable t)  {
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
        }

        return result;
    }

    @Override
    public List<String> createTableTriggersForMaterializedViews(BasicTable t) {
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

            String sql;
            //INSERT
            sql = String.format(
                    "CREATE TRIGGER \"" + insertTriggerName + "\" AFTER INSERT ON "
                            + tableString(t.getGrain().getName(), t.getName()) + " FOR EACH ROW CALL %n "
                            + MaterializedView.CHECKSUM_COMMENT_TEMPLATE + "%n"
                            + "\"%s\"",
                    mv.getChecksum(),
                    MaterializedViewInsertTrigger.class.getName());
            result.add(sql);
            this.rememberTrigger(query.withName(insertTriggerName));
            //UPDATE
            sql = String.format(
                    "CREATE TRIGGER \"" + updateTriggerName + "\" AFTER UPDATE ON "
                            + tableString(t.getGrain().getName(), t.getName()) + " FOR EACH ROW CALL \"%s\"",
                    MaterializedViewUpdateTrigger.class.getName());
            result.add(sql);
            this.rememberTrigger(query.withName(updateTriggerName));
            //DELETE
            sql = String.format(
                    "CREATE TRIGGER \"" + deleteTriggerName + "\" AFTER DELETE ON "
                            + tableString(t.getGrain().getName(), t.getName()) + " FOR EACH ROW CALL \"%s\"",
                    MaterializedViewDeleteTrigger.class.getName());
            result.add(sql);
            this.rememberTrigger(query.withName(deleteTriggerName));
        }

        return result;
    }

}

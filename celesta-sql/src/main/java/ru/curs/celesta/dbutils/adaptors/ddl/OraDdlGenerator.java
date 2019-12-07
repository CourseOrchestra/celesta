package ru.curs.celesta.dbutils.adaptors.ddl;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.DBType;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;


import ru.curs.celesta.dbutils.adaptors.column.ColumnDefiner;
import ru.curs.celesta.dbutils.adaptors.column.ColumnDefinerFactory;
import ru.curs.celesta.dbutils.adaptors.column.OraColumnDefiner;
import ru.curs.celesta.dbutils.jdbc.SqlUtils;
import ru.curs.celesta.dbutils.meta.DbColumnInfo;

import ru.curs.celesta.dbutils.meta.DbIndexInfo;
import ru.curs.celesta.event.TriggerQuery;
import ru.curs.celesta.event.TriggerType;
import ru.curs.celesta.score.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static ru.curs.celesta.dbutils.adaptors.constants.CommonConstants.ALTER_TABLE;

import static ru.curs.celesta.dbutils.adaptors.constants.OraConstants.CSC;
import static ru.curs.celesta.dbutils.adaptors.constants.OraConstants.DROP_TRIGGER;
import static ru.curs.celesta.dbutils.adaptors.constants.OraConstants.SNL;

import static ru.curs.celesta.dbutils.adaptors.function.CommonFunctions.getFieldList;

import static ru.curs.celesta.dbutils.adaptors.function.OraFunctions.fromOrToNClob;
import static ru.curs.celesta.dbutils.adaptors.function.OraFunctions.getBooleanCheckName;
import static ru.curs.celesta.dbutils.adaptors.function.OraFunctions.translateDate;

import static ru.curs.celesta.dbutils.adaptors.function.SchemalessFunctions.generateSequenceTriggerName;
import static ru.curs.celesta.dbutils.adaptors.function.SchemalessFunctions.getIncrementSequenceName;
import static ru.curs.celesta.dbutils.adaptors.function.SchemalessFunctions.getUpdTriggerName;

/**
 * Class for SQL generation of data definition of Oracle.
 */
public final class OraDdlGenerator extends DdlGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(OraDdlGenerator.class);

    public OraDdlGenerator(DBAdaptor dmlAdaptor) {
        super(dmlAdaptor);
    }

    @Override
    Optional<String> createSchema(String name) {
        // Does nothing for Oracle. Schemes are imitated by prefixes in the table names.
        return Optional.empty();
    }

    @Override
    List<String> dropParameterizedView(String schemaName, String viewName, Connection conn)  {
        List<String> result = new ArrayList<>();

        //Удаление функции
        String dropFunction = String.format("DROP FUNCTION %s", tableString(schemaName, viewName));
        result.add(dropFunction);
        //Удаление табличного типа
        String tableTypeName = String.format("%s_%s_t", schemaName, viewName);
        if (hasTypeInteractive(tableTypeName, "COLLECTION", conn)) {
            result.add(dropType(tableTypeName));
        }
        //Удаление объекта
        String objectTypeName = String.format("%s_%s_o", schemaName, viewName);
        if (hasTypeInteractive(objectTypeName, "OBJECT", conn)) {
            result.add(dropType(objectTypeName));
        }

        return result;
    }

    @Override
    List<String> dropIndex(Grain g, DbIndexInfo dBIndexInfo) {
        final String sql;
        if (dBIndexInfo.getIndexName().startsWith("##")) {
            sql = dropIndex(dBIndexInfo.getIndexName().substring(2));
        } else {
            sql = dropIndex(tableString(g.getName(), dBIndexInfo.getIndexName()));
        }
        return Arrays.asList(sql);
    }

    @Override
    List<String> dropUpdateRule(String fkName) {
        List<String> result = new ArrayList<>();

        TriggerQuery triggerQuery = new TriggerQuery();
        triggerQuery.withName(getFKTriggerName(SNL, fkName));
        result.add(dropTriggerSql(triggerQuery));

        triggerQuery.withName(getFKTriggerName(CSC, fkName));
        result.add(dropTrigger(triggerQuery));

        return result;
    }

    private static String getFKTriggerName(String prefix, String fkName) {
        String result = prefix + fkName;
        result = NamedElement.limitName(result);
        return result;
    }

    @Override
    String dropTriggerSql(TriggerQuery query) {
        String sql = String.format("drop trigger \"%s\"", query.getName());
        return sql;
    }

    private String dropIndex(String indexFullName) {
        return String.format("DROP INDEX %s", indexFullName);
    }

    private boolean hasTypeInteractive(String typeName, String typeCode, Connection conn)  {
        String sql = String.format(
                "SELECT TYPE_NAME from DBA_TYPES "
              + "WHERE owner = sys_context('userenv','session_user')\n"
                  + " and TYPECODE = '%s' and TYPE_NAME = '%s'",
                                   typeCode, typeName);

        try (ResultSet rs = SqlUtils.executeQuery(conn, sql)) {
            return rs.next();
        } catch (SQLException e) {
            throw new CelestaException(e);
        }
    }

    private String dropType(String typeName) {
        return String.format("DROP TYPE \"%s\"", typeName);
    }

    @Override
    String generateArgumentsForCreateSequenceExpression(
            SequenceElement s, SequenceElement.Argument... excludedArguments) {

        String result = super.generateArgumentsForCreateSequenceExpression(s, excludedArguments);
        if (s.hasArgument(SequenceElement.Argument.CYCLE)) {
            result = result + " NOCACHE";
        }
        return result;
    }

    @Override
    DBType getType() {
        return DBType.ORACLE;
    }

    @Override
    List<String> updateVersioningTrigger(Connection conn, TableElement t)  {
        List<String> result = new ArrayList<>();
        // First of all, we are about to check if trigger exists
        String triggerName = getUpdTriggerName(t);

        try {
            TriggerQuery query = new TriggerQuery()
                    .withSchema(t.getGrain().getName())
                    .withName(triggerName)
                    .withTableName(t.getName())
                    .withType(TriggerType.PRE_UPDATE);
            boolean triggerExists = this.triggerExists(conn, query);

            if (t instanceof VersionedElement) {
                VersionedElement ve = (VersionedElement) t;

                String sql;
                if (ve.isVersioned()) {
                    if (!triggerExists) {
                        // CREATE TRIGGER
                        sql = String.format("CREATE OR REPLACE TRIGGER \"%s\" BEFORE UPDATE ON \"%s_%s\" FOR EACH ROW\n"
                                          + "BEGIN\n"
                                          + "  IF :new.\"recversion\" <> :old.\"recversion\" THEN\n"
                                          + "    raise_application_error( -20001, 'record version check failure' );\n"
                                          + "  END IF;\n"
                                          + "  :new.\"recversion\" := :new.\"recversion\" + 1;\n"
                                          + "END;",
                                triggerName, t.getGrain().getName(), t.getName());
                        LOGGER.trace(sql);
                        result.add(sql);
                        this.rememberTrigger(query);
                    }
                } else {
                    if (triggerExists) {
                        // DROP TRIGGER
                        TriggerQuery dropQuery = new TriggerQuery().withName(triggerName);
                        result.add(dropTrigger(dropQuery));
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
    List<String> afterCreateTable(Connection conn, TableElement t) {
        List<String> result = new ArrayList<>();
        //creating of triggers to emulate default sequence values

        for (Column<?> column : t.getColumns().values()) {
            if (IntegerColumn.class.equals(column.getClass())) {
                IntegerColumn ic = (IntegerColumn) column;

                if (ic.getSequence() != null) {
                    SequenceElement s = ic.getSequence();
                    final String triggerName = generateSequenceTriggerName(ic);
                    final String sequenceName = sequenceString(s.getGrain().getName(), s.getName());
                    String sql = createOrReplaceSequenceTriggerForColumn(triggerName, ic, sequenceName);
                    result.add(sql);

                    TriggerQuery query = new TriggerQuery()
                            .withSchema(t.getGrain().getName())
                            .withTableName(t.getName())
                            .withName(triggerName);
                    this.rememberTrigger(query);
                }
            }
        }
        return result;
    }

    @Override
    public String dropPk(TableElement t, String pkName) {
        String sql = String.format("alter table \"%s_%s\" drop constraint \"%s\"", t.getGrain().getName(), t.getName(),
                pkName);
        return sql;
    }

    @Override
    List<String> updateColumn(Connection conn, Column<?> c, DbColumnInfo actual)  {
        @SuppressWarnings("unchecked")
        final Class<? extends Column<?>> cClass = (Class<Column<?>>) c.getClass();

        List<String> result = new ArrayList<>();

        final String tableFullName = tableString(c.getParentTable().getGrain().getName(), c.getParentTable().getName());

        TableElement t = c.getParentTable();
        String triggerName = getUpdTriggerName(t);
        TriggerQuery query = new TriggerQuery()
                .withSchema(t.getGrain().getName())
                .withName(triggerName)
                .withTableName(t.getName())
                .withType(TriggerType.PRE_UPDATE);


        boolean triggerExists = this.triggerExists(conn, query);
        if (triggerExists) {
            result.add(dropTrigger(query));
        }

        if (actual.getType() == BooleanColumn.class && !(c instanceof BooleanColumn)) {
            // Boolean type is being changed for something different. The constraint must be dropped.
            String sql = String.format(
                    ALTER_TABLE + tableFullName
                            + " drop constraint %s", getBooleanCheckName(c)
            );
            result.add(sql);
        }

        OraColumnDefiner definer = (OraColumnDefiner) ColumnDefinerFactory.getColumnDefiner(getType(), cClass);

        String defdef = defaultDefForAlter(c, definer, actual);

        // If blob-field is changed In Oracle then in alter table its type
        // should not be specified (there will be an error)
        String def;
        if (actual.getType() == BinaryColumn.class && c instanceof BinaryColumn) {
            def = OraColumnDefiner.join(c.getQuotedName(), defdef);
        } else {
            def = OraColumnDefiner.join(definer.getInternalDefinition(c), defdef);
        }

        // In Oracle nullable should be specified only if a change is really needed
        if (actual.isNullable() != c.isNullable()) {
            def = OraColumnDefiner.join(def, definer.nullable(c));
        }

        // A transfer from NCLOB and to NCLOB should be made with caution

        if (fromOrToNClob(c, actual)) {

            String tempName = "\"" + c.getName() + "2\"";
            String sql = String.format(
                    ALTER_TABLE + tableFullName + " add %s",
                    columnDef(c)
            );
            sql = sql.replace(c.getQuotedName(), tempName);
            LOGGER.trace(sql);
            result.add(sql);
            sql = String.format("update " + tableFullName + " set %s = \"%s\"",
                    tempName, c.getName());
            LOGGER.trace(sql);
            result.add(sql);
            sql = String.format(
                    ALTER_TABLE + tableFullName
                            + " drop column %s", c.getQuotedName()
            );
            LOGGER.trace(sql);
            result.add(sql);
            sql = String.format(
                    ALTER_TABLE + tableFullName
                            + " rename column %s to %s", tempName, c.getQuotedName());
            LOGGER.trace(sql);
            result.add(sql);
        } else if (actual.getType() == DecimalColumn.class && c instanceof DecimalColumn) {
            result.addAll(updateDecimalColumn(conn, (DecimalColumn) c, actual, def));
        } else {
            result.add(modifyColumn(tableFullName, def));
        }
        if (c instanceof BooleanColumn && actual.getType() != BooleanColumn.class) {
            // The type has been changed to Boolean, a constraint has to be set
            String sql = String.format(
                    ALTER_TABLE + tableFullName
                            + " add constraint %s check (%s in (0, 1))", getBooleanCheckName(c), c.getQuotedName()
            );
            result.add(sql);
        }
        if (c instanceof IntegerColumn) {
            IntegerColumn ic = (IntegerColumn) c;


            if ("".equals(actual.getDefaultValue())) { //old defaultValue Is null - create trigger if necessary
                if (((IntegerColumn) c).getSequence() != null) {
                    final String sequenceTriggerName = generateSequenceTriggerName(ic);
                    final String sequenceName = sequenceString(
                            c.getParentTable().getGrain().getName(), ic.getSequence().getName());
                    String sql = createOrReplaceSequenceTriggerForColumn(sequenceTriggerName, ic, sequenceName);
                    result.add(sql);

                    TriggerQuery q = new TriggerQuery()
                            .withSchema(t.getGrain().getName())
                            .withTableName(t.getName())
                            .withName(sequenceTriggerName);
                    this.rememberTrigger(q);
                }
            } else {
                Pattern p = Pattern.compile("(?i)NEXTVAL\\((.*)\\)");
                Matcher m = p.matcher(actual.getDefaultValue());

                if (m.matches()) { //old default value is sequence
                    if (ic.getSequence() == null) {
                        TriggerQuery triggerQuery = new TriggerQuery()
                                .withSchema(c.getParentTable().getGrain().getName())
                                .withTableName(c.getParentTable().getName())
                                .withName(generateSequenceTriggerName(ic))
                                .withType(TriggerType.PRE_INSERT);

                        triggerExists = this.triggerExists(conn, query);

                        if (triggerExists) {
                            result.add(dropTrigger(triggerQuery));
                        }
                    } else {
                        String oldSequenceName = m.group(1);

                        if (!oldSequenceName.equals(ic.getSequence().getName())) { //using of new sequence
                            final String sequenceName = sequenceString(
                                    c.getParentTable().getGrain().getName(), ic.getSequence().getName());
                            String sql = createOrReplaceSequenceTriggerForColumn(
                                    generateSequenceTriggerName(ic), ic, sequenceName);
                            result.add(sql);

                            TriggerQuery triggerQuery = new TriggerQuery()
                                    .withSchema(c.getParentTable().getGrain().getName())
                                    .withTableName(c.getParentTable().getName())
                                    .withName(generateSequenceTriggerName(ic))
                                    .withType(TriggerType.PRE_INSERT);

                            this.rememberTrigger(triggerQuery);
                        }
                    }
                } else if (ic.getSequence() != null) {
                    final String sequenceName = sequenceString(
                            c.getParentTable().getGrain().getName(), ic.getSequence().getName());
                    String sql = createOrReplaceSequenceTriggerForColumn(
                            generateSequenceTriggerName(ic), ic, sequenceName);
                    result.add(sql);
                }
            }
        }

        return result;
    }

    @Override
    List<String> createIndex(Index index) {
        String grainName = index.getTable().getGrain().getName();
        String fieldList = getFieldList(index.getColumns().keySet());
        String sql = String.format(
                "CREATE INDEX " + tableString(grainName, index.getName())
                        + " ON " + tableString(grainName, index.getTable().getName()) + " (%s)",
                fieldList
        );
        return Arrays.asList(sql);
    }

    private String createOrReplaceSequenceTriggerForColumn(
            String triggerName, IntegerColumn ic, String quotedSequenceName) {

        TableElement t = ic.getParentTable();
        String sql = String.format(
                "CREATE OR REPLACE TRIGGER \"" + triggerName + "\" BEFORE INSERT ON "
                        + tableString(t.getGrain().getName(), t.getName())
                        + " FOR EACH ROW WHEN (new.%s is null) BEGIN SELECT "
                        + quotedSequenceName + ".NEXTVAL INTO :new.%s FROM dual; END;",
                ic.getQuotedName(), ic.getQuotedName());

        return sql;
    }

    @Override
    void processCreateUpdateRule(Connection conn, ForeignKey fk, LinkedList<StringBuilder> sqlQueue)  {
        String snlTriggerName = getFKTriggerName(SNL, fk.getConstraintName());
        String cscTriggerName = getFKTriggerName(CSC, fk.getConstraintName());
        TriggerQuery query = new TriggerQuery()
                .withSchema(fk.getParentTable().getGrain().getName())
                .withTableName(fk.getParentTable().getName())
                .withName(snlTriggerName)
                .withType(TriggerType.POST_UPDATE);

        final boolean snlTriggerExists;
        final boolean cscTriggerExists;

        snlTriggerExists = this.triggerExists(conn, query);
        query.withName(cscTriggerName);
        cscTriggerExists = this.triggerExists(conn, query);

        if (snlTriggerExists || cscTriggerExists) {
            StringBuilder sb = new StringBuilder(DROP_TRIGGER)
                    .append("\"");
            // Clean up unwanted triggers
            switch (fk.getUpdateRule()) {
                case CASCADE:
                    if (snlTriggerExists) {
                        sb.append(getFKTriggerName(SNL, fk.getConstraintName()));
                        sb.append("\"");
                        sqlQueue.add(sb);
                        this.forgetTrigger(query.withName(snlTriggerName));
                    }
                    break;
                case SET_NULL:
                    if (cscTriggerExists) {
                        sb.append(getFKTriggerName(CSC, fk.getConstraintName()));
                        sb.append("\"");
                        sqlQueue.add(sb);
                        this.forgetTrigger(query.withName(cscTriggerName));
                    }
                    break;
                case NO_ACTION:
                default:
                    if (snlTriggerExists && cscTriggerExists) {
                        sb.append(getFKTriggerName(SNL, fk.getConstraintName()));
                        sb.append("\"");
                        sqlQueue.add(sb);
                        this.forgetTrigger(query.withName(snlTriggerName));
                        sb = new StringBuilder(DROP_TRIGGER);
                        sb.append(getFKTriggerName(CSC, fk.getConstraintName()));
                        sb.append("\"");
                        sqlQueue.add(sb);
                        this.forgetTrigger(query.withName(snlTriggerName));
                    }
                    return;
            }

        }

        StringBuilder sb = new StringBuilder();
        sb.append("create or replace trigger \"");
        if (fk.getUpdateRule() == FKRule.CASCADE) {
            String triggerName = getFKTriggerName(CSC, fk.getConstraintName());
            sb.append(triggerName);
            query.withName(triggerName);
        } else {
            String triggerName = getFKTriggerName(SNL, fk.getConstraintName());
            sb.append(triggerName);
            query.withName(triggerName);
        }
        sb.append("\" after update of ");
        BasicTable t = fk.getReferencedTable();
        boolean needComma = false;
        for (Column<?> c : t.getPrimaryKey().values()) {
            if (needComma) {
                sb.append(", ");
            }
            sb.append(c.getQuotedName());
            needComma = true;
        }
        sb.append(String.format(" on \"%s_%s\"", t.getGrain().getName(), t.getName()));
        sb.append(String.format(" for each row begin\n  update \"%s_%s\" set ",
                fk.getParentTable().getGrain().getName(), fk.getParentTable().getName()));

        Iterator<Column<?>> i1 = fk.getColumns().values().iterator();
        Iterator<Column<?>> i2 = t.getPrimaryKey().values().iterator();
        needComma = false;
        while (i1.hasNext()) {
            sb.append(needComma ? ",\n    " : "\n    ");
            needComma = true;
            sb.append(i1.next().getQuotedName());
            sb.append(" = :new.");
            sb.append(i2.next().getQuotedName());
        }
        sb.append("\n  where ");
        i1 = fk.getColumns().values().iterator();
        i2 = t.getPrimaryKey().values().iterator();
        needComma = false;
        while (i1.hasNext()) {
            sb.append(needComma ? ",\n    " : "\n    ");
            needComma = true;
            sb.append(i1.next().getQuotedName());
            if (fk.getUpdateRule() == FKRule.CASCADE) {
                sb.append(" = :old.");
                sb.append(i2.next().getQuotedName());
            } else {
                sb.append(" = null");
            }
        }
        sb.append(";\nend;");
        sqlQueue.add(sb);
        this.rememberTrigger(query);
    }

    @Override
    public SQLGenerator getViewSQLGenerator() {
        return new SQLGenerator() {

            @Override
            protected String viewName(AbstractView v) {
                return tableString(v.getGrain().getName(), v.getName());
            }

            @Override
            protected String tableName(TableRef tRef) {
                BasicTable t = tRef.getTable();
                return String.format(tableString(t.getGrain().getName(), t.getName()) + " \"%s\"", tRef.getAlias());
            }

            @Override
            protected String checkForDate(String lexValue) {
                try {
                    return translateDate(lexValue);
                } catch (CelestaException e) {
                    // This is not a date
                    return lexValue;
                }
            }

            @Override
            protected String boolLiteral(boolean val) {
                return val ? "1" : "0";
            }

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
    List<String> createParameterizedView(ParameterizedView pv)  {
        List<String> result = new ArrayList<>();

        // Create type
        String colsDef = pv.getColumns().entrySet().stream()
                .map(e -> {
                    StringBuilder sb = new StringBuilder("\"").append(e.getKey()).append("\" ")
                            .append(ColumnDefinerFactory.getColumnDefiner(getType(),
                                    CELESTA_TYPES_COLUMN_CLASSES.get(e.getValue().getCelestaType())
                            ).dbFieldType());

                    Column<?> colRef = pv.getColumnRef(e.getKey());

                    if (colRef != null && StringColumn.VARCHAR.equals(colRef.getCelestaType())) {
                        StringColumn sc = (StringColumn) colRef;
                        sb.append("(").append(sc.getLength()).append(")");
                    }

                    return sb.toString();
                }).collect(Collectors.joining(",\n"));

        String sql = String.format(
                "create type " + tableString(pv.getGrain().getName(), pv.getName() + "_o")
                        + " as object\n" + "(%s)", colsDef
        );
        LOGGER.trace(sql);
        result.add(sql);

        // Create collection of types
        sql = "create type " + tableString(pv.getGrain().getName(), pv.getName() + "_t")
                + " as TABLE OF " + tableString(pv.getGrain().getName(), pv.getName() + "_o");
        LOGGER.trace(sql);
        result.add(sql);

        // Create function
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
                        e.getKey() + " IN "
                                + ColumnDefinerFactory.getColumnDefiner(getType(),
                                CELESTA_TYPES_COLUMN_CLASSES.get(e.getValue().getType().getCelestaType())
                        ).dbFieldType()

                ).collect(Collectors.joining(", "));

        String selectSql = sw.toString();

        String objectParams = pv.getColumns().keySet().stream()
                .map(alias -> "curr.\"" + alias + "\"")
                .collect(Collectors.joining(", "));

        sql = String.format(
                "create or replace function " + tableString(pv.getGrain().getName(), pv.getName()) + "(%s) return "
                        + tableString(pv.getGrain().getName(), pv.getName() + "_t")
                        + " PIPELINED IS\n"
                        + "BEGIN\n"
                        + "for curr in (%s) loop \n"
                        + "pipe row (%s(%s));\n"
                        + "end loop;"
                        + "END;",
                pvParams,
                selectSql, tableString(pv.getGrain().getName(), pv.getName() + "_o"),
                objectParams);
        result.add(sql);

        return result;
    }

    @Override
    Optional<String> dropAutoIncrement(Connection conn, TableElement t)  {
        String sequenceName = getIncrementSequenceName(t);
        String sequenceExistsSql = String.format(
                "select count(*) from user_sequences where sequence_name = '%s'",
                sequenceName
        );

        final boolean incSequenceExists;

        try (Statement checkForTable = conn.createStatement();
            ResultSet rs = checkForTable.executeQuery(sequenceExistsSql)) {
            incSequenceExists = rs.next() && rs.getInt(1) > 0;
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage());
        }

        if (incSequenceExists) {
            String sql = String.format("DROP SEQUENCE \"%s\"", sequenceName);
            return Optional.of(sql);
        } else {
            return Optional.empty();
        }
    }

    @Override
    String truncDate(String dateStr) {
        return "TRUNC(" + dateStr + " , 'DD')";
    }

    @Override
    public List<String> dropTableTriggersForMaterializedViews(Connection conn, BasicTable t)  {
        List<String> result = new ArrayList<>();

        List<MaterializedView> mvList = t.getGrain().getElements(MaterializedView.class).values().stream()
                .filter(mv -> mv.getRefTable().getTable().equals(t))
                .collect(Collectors.toList());

        for (MaterializedView mv : mvList) {
            TriggerQuery query = new TriggerQuery().withSchema(t.getGrain().getName())
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

        String fullTableName = tableString(t.getGrain().getName(), t.getName());

        TriggerQuery query = new TriggerQuery()
                .withSchema(t.getGrain().getName())
                .withTableName(t.getName());

        for (MaterializedView mv : mvList) {
            String fullMvName = tableString(mv.getGrain().getName(), mv.getName());

            String insertTriggerName = mv.getTriggerName(TriggerType.POST_INSERT);
            String updateTriggerName = mv.getTriggerName(TriggerType.POST_UPDATE);
            String deleteTriggerName = mv.getTriggerName(TriggerType.POST_DELETE);

            String lockTable = String.format("LOCK TABLE %s IN EXCLUSIVE MODE;\n", fullMvName);

            String mvColumns = mv.getColumns().keySet().stream()
                    .filter(alias -> !MaterializedView.SURROGATE_COUNT.equals(alias))
                    .map(alias -> "\"" + alias + "\"")
                    .collect(Collectors.joining(", "));

            String selectFromRowTemplate = mv.getColumns().keySet().stream()
                    .filter(alias -> !MaterializedView.SURROGATE_COUNT.equals(alias))
                    .map(alias -> {
                        Column<?> colRef = mv.getColumnRef(alias);

                        if (colRef == null) {
                            Map<String, Expr> aggrCols = mv.getAggregateColumns();
                            if (aggrCols.containsKey(alias) && aggrCols.get(alias) instanceof Count) {
                                return "1 as \"" + alias + "\"";
                            }
                            return "";
                        }

                        if (DateTimeColumn.CELESTA_TYPE.equals(colRef.getCelestaType())) {
                            return "TRUNC(%1$s.\"" + mv.getColumnRef(alias).getName() + "\", 'DD') as \"" + alias + "\"";
                        }

                        return "%1$s.\"" + mv.getColumnRef(alias).getName() + "\" as \"" + alias + "\"";
                    })
                    .filter(str -> !str.isEmpty())
                    .collect(Collectors.joining(", "));


            String rowColumnsTemplate = mv.getColumns().keySet().stream()
                    .filter(alias -> !MaterializedView.SURROGATE_COUNT.equals(alias))
                    .map(alias -> "%1$s.\"" + alias + "\"")
                    .collect(Collectors.joining(", "));

            String rowConditionTemplate = mv.getColumns().keySet().stream()
                    .filter(alias -> mv.isGroupByColumn(alias))
                    .map(alias -> "mv.\"" + alias + "\" = %1$s.\"" + alias + "\"")
                    .collect(Collectors.joining(" AND "));

            String rowConditionTemplateForDelete = mv.getColumns().keySet().stream()
                    .filter(alias -> mv.isGroupByColumn(alias))
                    .map(alias -> {
                        Column<?> colRef = mv.getColumnRef(alias);

                        if (DateTimeColumn.CELESTA_TYPE.equals(colRef.getCelestaType())) {
                            return "mv.\"" + alias + "\" = TRUNC(%1$s.\"" + mv.getColumnRef(alias).getName() + "\", 'DD')";
                        }

                        return "mv.\"" + alias + "\" = %1$s.\"" + mv.getColumnRef(alias).getName() + "\"";
                    })
                    .collect(Collectors.joining(" AND "));

            String setStatementTemplate = mv.getAggregateColumns().entrySet().stream()
                    .map(e -> {
                        StringBuilder sb = new StringBuilder();
                        String alias = e.getKey();

                        sb.append("mv.\"").append(alias)
                                .append("\" = mv.\"").append(alias)
                                .append("\" %1$s ");

                        if (e.getValue() instanceof Sum) {
                            sb.append("%2$s.\"").append(alias).append("\"");
                        } else if (e.getValue() instanceof Count) {
                            sb.append("1");
                        }

                        return sb.toString();
                    }).collect(Collectors.joining(", "))
                    .concat(", mv.\"").concat(MaterializedView.SURROGATE_COUNT).concat("\" = ")
                    .concat("mv.\"").concat(MaterializedView.SURROGATE_COUNT).concat("\" %1$s 1");


            String setStatementTemplateForDelete = mv.getAggregateColumns().entrySet().stream()
                    .map(e -> {
                        StringBuilder sb = new StringBuilder();
                        String alias = e.getKey();

                        sb.append("mv.\"").append(alias)
                                .append("\" = mv.\"").append(alias)
                                .append("\" %1$s ");

                        if (e.getValue() instanceof Sum) {
                            sb.append("%2$s.\"").append(mv.getColumnRef(alias).getName()).append("\"");
                        } else if (e.getValue() instanceof Count) {
                            sb.append("1");
                        }

                        return sb.toString();
                    }).collect(Collectors.joining(", "))
                    .concat(", mv.\"").concat(MaterializedView.SURROGATE_COUNT).concat("\" = ")
                    .concat("mv.\"").concat(MaterializedView.SURROGATE_COUNT).concat("\" %1$s 1");


            StringBuilder insertSqlBuilder = new StringBuilder("MERGE INTO %s mv \n")
                    .append("USING (SELECT %s FROM dual) \"inserted\" ON (%s) \n")
                    .append("WHEN MATCHED THEN \n ")
                    .append("UPDATE SET %s \n")
                    .append("WHEN NOT MATCHED THEN \n")
                    .append("INSERT (%s) VALUES (%s); \n");

            String insertSql = String.format(insertSqlBuilder.toString(), fullMvName,
                    String.format(selectFromRowTemplate, ":new"), String.format(rowConditionTemplate, "\"inserted\""),
                    String.format(setStatementTemplate, "+", "\"inserted\""),
                    mvColumns + ", \"" + MaterializedView.SURROGATE_COUNT + "\"",
                    String.format(rowColumnsTemplate, "\"inserted\"") + ", 1");

            String delStatement = String.format("mv.\"%s\" = 0", MaterializedView.SURROGATE_COUNT);

            StringBuilder deleteSqlBuilder = new StringBuilder(String.format("UPDATE %s mv \n", fullMvName))
                    .append("SET ").append(String.format(setStatementTemplateForDelete, "-", ":old")).append(" ")
                    .append("WHERE ").append(String.format(rowConditionTemplateForDelete, ":old")).append(";\n")
                    .append(String.format("DELETE FROM %s mv ", fullMvName))
                    .append("WHERE ").append(delStatement).append(";\n");


            String sql;

            //INSERT
            sql = String.format(
                    "create or replace trigger \"%s\" after insert "
                            + "on %s for each row\n"
                            + "begin \n" + MaterializedView.CHECKSUM_COMMENT_TEMPLATE
                            + "\n %s \n %s \n END;",
                    insertTriggerName, fullTableName, mv.getChecksum(), lockTable, insertSql);
            LOGGER.trace(sql);
            result.add(sql);
            this.rememberTrigger(query.withName(insertTriggerName));

            //UPDATE
            sql = String.format(
                    "create or replace trigger \"%s\" after update "
                            + "on %s for each row\n"
                            + "begin %s \n %s\n %s\n END;",
                    updateTriggerName, fullTableName, lockTable, deleteSqlBuilder.toString(), insertSql);

            LOGGER.trace(sql);
            result.add(sql);
            this.rememberTrigger(query.withName(updateTriggerName));

            //DELETE
            sql = String.format(
                    "create or replace trigger \"%s\" after delete "
                            + "on %s for each row\n "
                            + " begin %s \n %s\n END;",
                    deleteTriggerName, fullTableName, lockTable, deleteSqlBuilder.toString());

            result.add(sql);
            this.rememberTrigger(query.withName(deleteTriggerName));
        }

        return result;
    }

    private List<String> updateDecimalColumn(Connection conn, DecimalColumn dc, DbColumnInfo actual, String def) {
        List<String> result = new ArrayList<>();
        final String tableFullName = tableString(
                dc.getParentTable().getGrain().getName(), dc.getParentTable().getName());
        //If there is any decreasing of scale or whole part, we must use additional column to perform alter.
        int actualScale = actual.getScale(), scale = dc.getScale();
        int actualWholePartLength = actual.getLength() - actualScale,
                wholePartLength = dc.getPrecision() - scale;

        if (scale < actualScale || wholePartLength < actualWholePartLength) {
            if (!actual.isNullable()) {
                result.add(
                        String.format(
                                "alter table %s modify (%s null)", tableFullName, dc.getQuotedName()
                        )
                );
            }

            String tempColumnName = String.format(
                    "\"%s\"",
                    NamedElement.limitName(String.format("temp%s%s", dc.getName(), UUID.randomUUID().toString()))
            );

            OraColumnDefiner columnDefiner =
                    (OraColumnDefiner) ColumnDefinerFactory.getColumnDefiner(getType(), dc.getClass());

            String sql = String.format(
                    ALTER_TABLE + " %s add %s %s(%s,%s)",
                    tableFullName, tempColumnName, columnDefiner.dbFieldType(), dc.getPrecision(), dc.getScale()
            );
            result.add(sql);
            sql = String.format("update %s set %s = %s", tableFullName, tempColumnName, dc.getQuotedName());
            result.add(sql);
            sql = String.format("update %s set %s = null", tableFullName, dc.getQuotedName());
            result.add(sql);

            final String fillColumnSql = String.format(
                    "update %s set %s = %s", tableFullName, dc.getQuotedName(), tempColumnName
            );

            String selectSql = String.format("select count(*) from %s where %s is null",
                    tableFullName, dc.getQuotedName());

            final boolean hasNullValues;

            try (ResultSet rs = SqlUtils.executeQuery(conn, selectSql)) {
                rs.next();
                hasNullValues = rs.getInt(1) > 0;
            } catch (SQLException e) {
                throw new CelestaException(e);
            }

            if (!dc.isNullable() && !hasNullValues) {
                //Modify column without nullable flag to avoid error during altering.
                String defdef = defaultDefForAlter(dc, columnDefiner, actual);
                String preDef = OraColumnDefiner.join(columnDefiner.getInternalDefinition(dc), defdef);
                result.add(modifyColumn(tableFullName, preDef));

                //Fill records and finish modifying
                result.add(fillColumnSql);
                result.add(modifyColumn(tableFullName, def));
            } else {
                result.add(modifyColumn(tableFullName, def));
                result.add(fillColumnSql);
            }
            sql = String.format("alter table %s drop column %s", tableFullName, tempColumnName);
            result.add(sql);
        } else {
            result.add(modifyColumn(tableFullName, def));
        }

        return result;
    }

    private String modifyColumn(String tableFullName, String columnDef) {
        return String.format(ALTER_TABLE + tableFullName + " modify (%s)", columnDef);
    }

    private String defaultDefForAlter(Column<?> c, ColumnDefiner cd, DbColumnInfo actual) {
        // In Oracle you cannot drop Default, you can only set it to Null
        String result = cd.getDefaultDefinition(c);
        if ("".equals(result) && !"".equals(actual.getDefaultValue())) {
            result = "default null";
        }
        return result;
    }

}

package ru.curs.celesta.dbutils.adaptors.ddl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.DBType;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.dbutils.adaptors.column.ColumnDefinerFactory;
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
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ru.curs.celesta.dbutils.adaptors.constants.CommonConstants.ALTER_TABLE;


public class FirebirdDdlGenerator extends DdlGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(FirebirdDdlGenerator.class);

    public FirebirdDdlGenerator(DBAdaptor dmlAdaptor) {
        super(dmlAdaptor);
    }


    @Override
    List<String> createSequence(SequenceElement s) {
        List<String> result = new ArrayList<>();

        String createSql = String.format(
            "CREATE SEQUENCE %s",
            sequenceString(s.getGrain().getName(), s.getName())
        );

        result.add(createSql);

        if (s.getArguments().containsKey(SequenceElement.Argument.START_WITH)) {
            Long startWith = (Long)s.getArguments().get(SequenceElement.Argument.START_WITH) - 1;

            String startWithSql = String.format(
                "ALTER SEQUENCE %s RESTART WITH %s",
                sequenceString(s.getGrain().getName(), s.getName()),
                startWith
            );

            result.add(startWithSql);
        }

        return result;
    }

    @Override
    List<String> afterCreateTable(TableElement t) {
        List<String> result = new ArrayList<>();
        //creating of triggers to emulate default sequence values

        for (Column column : t.getColumns().values()) {
            if (IntegerColumn.class.equals(column.getClass())) {
                IntegerColumn ic = (IntegerColumn) column;

                if (ic.getSequence() != null) {
                    SequenceElement s = ic.getSequence();

                    final String triggerName = String.format(
                        //TODO:: WE NEED A FUNCTION FOR SEQUENCE TRIGGER NAME GENERATION
                        "%s_%s_%s_seq_trigger",
                        t.getGrain().getName(), t.getName(), ic.getName()
                    );

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
    List<String> dropParameterizedView(String schemaName, String viewName, Connection conn) {
        String sql = String.format(
            "DROP PROCEDURE %s",
            tableString(schemaName, viewName)
        );

        return Arrays.asList(sql);
    }

    @Override
    List<String> dropIndex(Grain g, DbIndexInfo dBIndexInfo) {
        String sql = String.format(
            "DROP INDEX %s",
            tableString(g.getName(), dBIndexInfo.getIndexName())
        );

        return Arrays.asList(sql);
    }

    @Override
    String dropTriggerSql(TriggerQuery query) {
        return String.format("DROP TRIGGER \"%s\"", query.getName());
    }

    @Override
    public String dropPk(TableElement t, String pkName) {

        return String.format(
            "ALTER TABLE %s DROP CONSTRAINT \"%s\"",
            this.tableString(t.getGrain().getName(), t.getName()),
            pkName
        );
    }

    @Override
    DBType getType() {
        return DBType.FIREBIRD;
    }

    @Override
    List<String> updateVersioningTrigger(Connection conn, TableElement t) {
        List<String> result = new ArrayList<>();

        // TODO:: NEED FUNCTION FOR THIS NAME
        String triggerName = String.format("%s_%s_version_check", t.getGrain().getName(), t.getName());

        // First of all, we are about to check if trigger exists
        try {
            TriggerQuery query = new TriggerQuery().withSchema(t.getGrain().getName())
                .withName(triggerName)
                .withTableName(t.getName());
            boolean triggerExists = this.triggerExists(conn, query);

            if (t instanceof VersionedElement) {
                VersionedElement ve = (VersionedElement) t;

                String sql;
                if (ve.isVersioned()) {
                    if (!triggerExists) {
                        // CREATE TRIGGER
                        sql =
                            "CREATE TRIGGER \"" + triggerName + "\" " +
                                "for " + tableString(t.getGrain().getName(), t.getName())
                                + " BEFORE UPDATE \n"
                                + " AS \n"
                                + " BEGIN \n"
                                + "   IF (OLD.\"recversion\" = NEW.\"recversion\")\n"
                                + "     THEN NEW.\"recversion\" = NEW.\"recversion\" + 1;"
                                + "   ELSE "
                                + "     EXCEPTION VERSION_CHECK_ERROR;"
                                + " END";
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
    List<String> createIndex(Index index) {
        String indexColumns = index.getColumns().values()
            .stream()
            .map(Column::getQuotedName)
            .collect(Collectors.joining(", "));
        String sql = String.format(
            "CREATE INDEX %s ON %s (%s)",
            tableString(index.getTable().getGrain().getName(), index.getName()),
            this.tableString(index.getTable().getGrain().getName(), index.getTable().getName()),
            indexColumns
        );

        return Arrays.asList(sql);
    }

    @Override
    List<String> updateColumn(Connection conn, Column c, DbColumnInfo actual) {
        final Class<? extends Column<?>> cClass = (Class<Column<?>>) c.getClass();

        List<String> result = new ArrayList<>();

        final String tableFullName = tableString(c.getParentTable().getGrain().getName(), c.getParentTable().getName());

        TableElement t = c.getParentTable();
        //TODO:: WE NEED A FUNCTION FOR SEQUENCE TRIGGER NAME GENERATION
        String triggerName = String.format("%s_%s_version_check", t.getGrain().getName(), t.getName());

        TriggerQuery query = new TriggerQuery()
            .withSchema(t.getGrain().getName())
            .withName(triggerName)
            .withTableName(t.getName());


        boolean triggerExists = this.triggerExists(conn, query);
        if (triggerExists) {
            result.add(dropTrigger(query));
        }

        String sql;

        Matcher nextValMatcher = Pattern.compile(DbColumnInfo.SEQUENCE_NEXT_VAL_PATTERN)
            .matcher(actual.getDefaultValue());

        // Starting with deletion of default-value if exists
        if (!actual.getDefaultValue().isEmpty() && !nextValMatcher.matches()) {
            sql = String.format(
                ALTER_TABLE + tableFullName
                    + " ALTER COLUMN \"%s\" DROP DEFAULT",
                c.getName()
            );
            result.add(sql);
        }

        result.addAll(this.updateColType(c, actual));

        // Checking for nullability
        if (c.isNullable() != actual.isNullable()) {
            sql = String.format(
                ALTER_TABLE + tableString(c.getParentTable().getGrain().getName(), c.getParentTable().getName())
                    + " ALTER COLUMN \"%s\" %s",
                c.getName(), c.isNullable() ? "DROP NOT NULL" : "SET NOT NULL");
            result.add(sql);
        }

        // If there's an empty default in data, and non-empty one in metadata then
        if (c.getDefaultValue() != null || (c instanceof DateTimeColumn && ((DateTimeColumn) c).isGetdate()))
        {
            sql = String.format(
                ALTER_TABLE + tableString(c.getParentTable().getGrain().getName(), c.getParentTable().getName())
                    + " ALTER COLUMN \"%s\" SET %s",
                c.getName(), ColumnDefinerFactory.getColumnDefiner(getType(), cClass).getDefaultDefinition(c));
            result.add(sql);
        }

        //TODO:: COPY-PASTE
        if (c instanceof IntegerColumn) {
            IntegerColumn ic = (IntegerColumn) c;




            if ("".equals(actual.getDefaultValue())) { //old defaultValue Is null - create trigger if necessary
                if (((IntegerColumn) c).getSequence() != null) {
                    final String sequenceTriggerName = String.format(
                        //TODO:: WE NEED A FUNCTION FOR SEQUENCE TRIGGER NAME GENERATION
                        "%s_%s_%s_seq_trigger",
                        t.getGrain().getName(), t.getName(), ic.getName()
                    );
                    final String sequenceName = sequenceString(
                        c.getParentTable().getGrain().getName(), ic.getSequence().getName());
                    sql = createOrReplaceSequenceTriggerForColumn(sequenceTriggerName, ic, sequenceName);
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
                            .withName(String.format(
                                //TODO:: WE NEED A FUNCTION FOR SEQUENCE TRIGGER NAME GENERATION
                                "%s_%s_%s_seq_trigger",
                                t.getGrain().getName(), t.getName(), ic.getName()
                            ))
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
                            sql = createOrReplaceSequenceTriggerForColumn(
                                String.format(
                                    //TODO:: WE NEED A FUNCTION FOR SEQUENCE TRIGGER NAME GENERATION
                                    "%s_%s_%s_seq_trigger",
                                    t.getGrain().getName(), t.getName(), ic.getName()
                                ), ic, sequenceName);
                            result.add(sql);

                            TriggerQuery triggerQuery = new TriggerQuery()
                                .withSchema(c.getParentTable().getGrain().getName())
                                .withTableName(c.getParentTable().getName())
                                .withName(String.format(
                                    //TODO:: WE NEED A FUNCTION FOR SEQUENCE TRIGGER NAME GENERATION
                                    "%s_%s_%s_seq_trigger",
                                    t.getGrain().getName(), t.getName(), ic.getName()
                                ))
                                .withType(TriggerType.PRE_INSERT);

                            this.rememberTrigger(triggerQuery);
                        }
                    }
                } else if (ic.getSequence() != null) {
                    final String sequenceName = sequenceString(
                        c.getParentTable().getGrain().getName(), ic.getSequence().getName());
                    sql = createOrReplaceSequenceTriggerForColumn(
                        String.format(
                            //TODO:: WE NEED A FUNCTION FOR SEQUENCE TRIGGER NAME GENERATION
                            "%s_%s_%s_seq_trigger",
                            t.getGrain().getName(), t.getName(), ic.getName()
                        ), ic, sequenceName);
                    result.add(sql);
                }
            }
        }
        // TODO:: END COPY-PASTE

        return result;
    }

    @Override
    String truncateTable(String tableName) {
        return String.format("DELETE FROM %s", tableName);
    }

    @Override
    SQLGenerator getViewSQLGenerator() {
        return new SQLGenerator() {
            @Override
            protected String preamble(AbstractView view) {
                return String.format("create view %s as", viewName(view));
            }

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
            protected String getDate() {
                return "CURRENT_TIMESTAMP";
            }

            @Override
            protected String paramLiteral(String paramName) {
                return ":" + paramName;
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

        // Calculating of max available varchar length for input params
        Map<String, Integer> textParamToLengthMap = Stream.of((LogicValuedExpr) pv.getWhereCondition())
            .map(logicValuedExpr -> new BaseLogicValuedExprExtractor().extract(logicValuedExpr))
            .flatMap(List::stream)
            .filter(logicValuedExpr -> {
                Set<Class<? extends Expr>> opsClasses = logicValuedExpr.getAllOperands().stream()
                    .map(Expr::getClass)
                    .collect(Collectors.toSet());

                return opsClasses.containsAll(Arrays.asList(ParameterRef.class, FieldRef.class));
            })
            .map(logicValuedExpr -> {
                Map<Class, List<Expr>> classToExprsMap = logicValuedExpr.getAllOperands().stream()
                    .collect(Collectors.toMap(
                        Expr::getClass,
                        expr -> new ArrayList<>(Arrays.asList(expr)),
                        (oldList, newList) -> Stream.of(oldList, newList).flatMap(List::stream).collect(Collectors.toList())
                    ));

                return classToExprsMap;
                }
            ).filter(classExprMap ->
                classExprMap.get(ParameterRef.class).stream()
                .anyMatch(expr -> ViewColumnType.TEXT.equals(expr.getMeta().getColumnType()))
            )
            .map(classExprMap -> {
                    Map<Class<? extends Expr>, List<Expr>> result = new HashMap<>();
                    result.put(
                        ParameterRef.class,
                        classExprMap.get(ParameterRef.class).stream()
                            .map(ParameterRef.class::cast)
                            .filter(parameterRef -> ViewColumnType.TEXT.equals(parameterRef.getMeta().getColumnType()))
                            .collect(Collectors.toList())
                    );
                    result.put(
                        FieldRef.class,
                        classExprMap.get(FieldRef.class).stream()
                            .map(FieldRef.class::cast)
                            .filter(fieldRef -> fieldRef.getColumn() instanceof StringColumn)
                            .collect(Collectors.toList())
                    );
                    return result;
                })
            .map(classExprMap -> classExprMap.get(ParameterRef.class).stream()
                .map(ParameterRef.class::cast)
                .collect(Collectors.toMap(
                    Function.identity(),
                    pr -> classExprMap.get(FieldRef.class).stream()
                        .map(FieldRef.class::cast)
                        .collect(Collectors.toList())
                )))
            .flatMap(map -> map.entrySet().stream())
            .map(e -> e.getValue().stream()
                .map(FieldRef::getColumn)
                .map(StringColumn.class::cast)
                .map(sc -> new AbstractMap.SimpleEntry<>(e.getKey(), sc))
                .collect(Collectors.toList())
            )
            .flatMap(List::stream)
            .collect(
                Collectors.toMap(
                    e -> e.getKey().getName(),
                    e -> e.getValue().isMax() ? 0 : e.getValue().getLength(),
                    (oldLength, newLength) -> {
                        if (oldLength == 0 || newLength == 0) {
                            return 0;
                        } else {
                            return Math.max(oldLength, newLength);
                        }
                    }
                )
            );

        String inParams = pv.getParameters()
            .entrySet().stream()
            .map(e -> {
                    final String type;

                    ViewColumnType viewColumnType = e.getValue().getType();
                    if (ViewColumnType.TEXT == viewColumnType) {
                        int length = textParamToLengthMap.get(e.getKey());

                        if (length == 0) {
                            type = "blob sub_type text";
                        } else {
                            type = String.format("varchar(%d)", length);
                        }
                    } else {
                        type = ColumnDefinerFactory.getColumnDefiner(getType(),
                            CELESTA_TYPES_COLUMN_CLASSES.get(e.getValue().getType().getCelestaType())
                        ).dbFieldType();
                    }

                    return e.getKey() + " " + type;
                }
            ).collect(Collectors.joining(", "));


        String outParams = pv.getColumns()
            .entrySet().stream()
            .map(e -> {
                    final String type;

                    ViewColumnMeta viewColumnMeta = e.getValue();
                    if (ViewColumnType.TEXT == viewColumnMeta.getColumnType()) {
                        StringColumn sc = (StringColumn)pv.getColumnRef(viewColumnMeta.getName());

                        if (sc.isMax()) {
                            type = "blob sub_type text";
                        } else {
                            type = String.format("varchar(%d)", sc.getLength());
                        }
                    } else {
                        type = ColumnDefinerFactory.getColumnDefiner(getType(),
                            CELESTA_TYPES_COLUMN_CLASSES.get(e.getValue().getCelestaType())
                        ).dbFieldType();
                    }

                    return e.getKey() + " " + type;
            }
            ).collect(Collectors.joining(", "));

        String intoList = pv.getColumns().keySet().stream()
            .map(":"::concat)
            .collect(Collectors.joining(", "));

        String selectSql = sw.toString();

        String sql = String.format(
            "CREATE PROCEDURE " + tableString(pv.getGrain().getName(), pv.getName()) + "(%s)\n"
                + "  RETURNS (%s)\n"
                + "  AS\n"
                + "  BEGIN\n"
                + "  FOR %s\n"
                + "  INTO %s\n"
                + "    DO BEGIN\n"
                + "      SUSPEND;\n"
                + "    END\n"
                + "  END",
            inParams, outParams, selectSql, intoList);

        return Arrays.asList(sql);
    }

    public static class BaseLogicValuedExprExtractor {
        List<LogicValuedExpr> extract(LogicValuedExpr logicValuedExpr) {
            List<LogicValuedExpr> result = new ArrayList<>();

            boolean containsAnotherLogicValuedExpr = logicValuedExpr.getAllOperands().stream()
                .anyMatch(expr -> expr instanceof LogicValuedExpr);

            if (containsAnotherLogicValuedExpr) {
                logicValuedExpr.getAllOperands().stream()
                    .filter(expr -> expr instanceof LogicValuedExpr)
                    .map(LogicValuedExpr.class::cast)
                    .forEach(lve -> result.addAll(this.extract(lve)));
            } else {
                result.add(logicValuedExpr);
            }

            return result;
        }
    }

    @Override
    Optional<String> dropAutoIncrement(Connection conn, TableElement t) {
        return Optional.empty();
    }

    @Override
    public List<String> dropTableTriggersForMaterializedViews(Connection conn, BasicTable t) {
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
        // TODO:: What about locks?
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

            String mvColumns = mv.getColumns().keySet().stream()
                .filter(alias -> !MaterializedView.SURROGATE_COUNT.equals(alias))
                .collect(Collectors.joining(", "))
                .concat(", " + MaterializedView.SURROGATE_COUNT);

            String aggregateColumns = mv.getColumns().keySet().stream()
                .filter(alias -> !MaterializedView.SURROGATE_COUNT.equals(alias))
                .map(alias -> "aggregate." + alias)
                .collect(Collectors.joining(", "))
                .concat(", " + MaterializedView.SURROGATE_COUNT);

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

            String tableGroupByColumns = mv.getColumns().values().stream()
                .filter(v -> mv.isGroupByColumn(v.getName()))
                .map(v -> "\"" + mv.getColumnRef(v.getName()).getName() + "\"")
                .collect(Collectors.joining(", "));

            String rowConditionTemplate = mv.getColumns().keySet().stream()
                .filter(alias -> mv.isGroupByColumn(alias))
                .map(alias -> "mv." + alias + " = %1$s." + alias + " ")
                .collect(Collectors.joining(" AND "));

            StringBuilder insertSqlBuilder = new StringBuilder("MERGE INTO %s mv ")
                .append("USING (SELECT %s FROM inserted GROUP BY %s) AS aggregate ON %s \n")
                .append("WHEN MATCHED THEN \n ")
                .append("UPDATE SET %s \n")
                .append("WHEN NOT MATCHED THEN \n")
                .append("INSERT (%s) VALUES (%s); \n");

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

            String insertSql = String.format(insertSqlBuilder.toString(), fullMvName,
                selectPartOfScript, tableGroupByColumns, String.format(rowConditionTemplate, "aggregate"),
                String.format(setStatementTemplate, "+"), mvColumns, aggregateColumns);

            String sql =
                "CREATE TRIGGER \"" + insertTriggerName + "\" " +
                    "for " + tableString(t.getGrain().getName(), t.getName())
                    + " AFTER INSERT \n"
                    + " AS \n"
                    + " BEGIN \n"
                    + MaterializedView.CHECKSUM_COMMENT_TEMPLATE
                    + "\n " + insertSql + "\n END;";

            //result.add(sql);
        }

        return result;
    }

    @Override
    String truncDate(String dateStr) {
        return String.format("CAST(CAST(%s as Date) AS TIMESTAMP)", dateStr);
    }


    private String createOrReplaceSequenceTriggerForColumn(String triggerName, IntegerColumn ic,
                                                           String quotedSequenceName) {
        TableElement te = ic.getParentTable();

        String sql =
            "CREATE TRIGGER \"" + triggerName + "\" " +
                "for " + tableString(te.getGrain().getName(), te.getName())
                + " BEFORE INSERT \n"
                + " AS \n"
                + " BEGIN \n"
                + "   IF (NEW." + ic.getQuotedName() + " IS NULL)\n"
                + "     THEN NEW." + ic.getQuotedName() + " = GEN_ID(" + quotedSequenceName + ", "
                + ic.getSequence().getArguments().get(SequenceElement.Argument.INCREMENT_BY) + ");"
                + " END";

        return sql;
    }

    private List<String> updateColType(Column<?> c, DbColumnInfo actual) {
        final List<String> result = new ArrayList<>();

        final Class<? extends Column<?>> cClass = (Class<Column<?>>) c.getClass();

        final String colType;
        final String fullTableName = tableString(
            c.getParentTable().getGrain().getName(),
            c.getParentTable().getName()
        );

        if (c.getClass() == StringColumn.class) {
            StringColumn sc = (StringColumn) c;

            colType = sc.isMax() ? "blob sub_type text" : String.format(
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
                ALTER_TABLE + fullTableName + " ALTER COLUMN \"%s\" TYPE %s",
                c.getName(),
                colType
            )
        );

        // If type doesn't match
        if (c.getClass() != actual.getType()) {
            if (c.getClass() == IntegerColumn.class && actual.getType() == StringColumn.class) {
                result.addAll(this.updateColTypeViaTempColumn(c, actual));
            } else if (c.getClass() == BooleanColumn.class) {
                result.addAll(this.updateColTypeViaTempColumn(c, actual));
            } else {
                result.add(alterSql.toString());
            }
        } else if (c.getClass() == StringColumn.class) {
            StringColumn sc = (StringColumn) c;

            if (actual.isMax() != sc.isMax()) {
                result.addAll(this.updateColTypeViaTempColumn(c, actual));
            } else if (sc.getLength() != actual.getLength()) {
                result.add(alterSql.toString());
            }

        } else if (c.getClass() == DecimalColumn.class) {
            DecimalColumn dc = (DecimalColumn) c;
            if (dc.getPrecision() != actual.getLength() || dc.getScale() != dc.getScale()) {
                result.addAll(this.updateColTypeViaTempColumn(c, actual));
            }
        }

        return result;
    }

    private List<String> updateColTypeViaTempColumn(Column<?> c, DbColumnInfo actual) {
        List<String> result = new ArrayList<>();

        final String fullTableName = tableString(
            c.getParentTable().getGrain().getName(),
            c.getParentTable().getName()
        );

        String tempColumnName = String.format("%s_temp", c.getName());

        String renameColumnSql = String.format(
            "ALTER TABLE %s\n" + " ALTER COLUMN %s TO %s",
            fullTableName,
            c.getQuotedName(),
            tempColumnName
        );

        String createColumnSql = String.format(
            "ALTER TABLE %s ADD %s",
            fullTableName,
            columnDef(c)
        );

        String copySql = String.format(
            "UPDATE %s SET %s = %s",
            fullTableName,
            c.getQuotedName(),
            tempColumnName
        );

        String deleteTempColumn = String.format(
            "ALTER TABLE %s DROP %s",
            fullTableName,
            tempColumnName
        );

        result.add(renameColumnSql);
        result.add(createColumnSql);
        result.add("COMMIT");
        result.add(copySql);
        result.add(deleteTempColumn);

        return result;
    }
}

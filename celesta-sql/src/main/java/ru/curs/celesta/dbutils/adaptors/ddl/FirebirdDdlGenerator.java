package ru.curs.celesta.dbutils.adaptors.ddl;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.DBType;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.dbutils.adaptors.FirebirdAdaptor;
import ru.curs.celesta.dbutils.adaptors.column.ColumnDefinerFactory;
import ru.curs.celesta.dbutils.meta.DbColumnInfo;
import ru.curs.celesta.dbutils.meta.DbIndexInfo;
import ru.curs.celesta.event.TriggerQuery;
import ru.curs.celesta.event.TriggerType;
import ru.curs.celesta.score.AbstractView;
import ru.curs.celesta.score.BasicTable;
import ru.curs.celesta.score.BinaryTermOp;
import ru.curs.celesta.score.BooleanColumn;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.Count;
import ru.curs.celesta.score.DateTimeColumn;
import ru.curs.celesta.score.DecimalColumn;
import ru.curs.celesta.score.Expr;
import ru.curs.celesta.score.FieldRef;
import ru.curs.celesta.score.ForeignKey;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.Index;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.LogicValuedExpr;
import ru.curs.celesta.score.MaterializedView;
import ru.curs.celesta.score.ParameterRef;
import ru.curs.celesta.score.ParameterizedView;
import ru.curs.celesta.score.ParameterizedViewSelectStmt;
import ru.curs.celesta.score.SQLGenerator;
import ru.curs.celesta.score.SequenceElement;
import ru.curs.celesta.score.StringColumn;
import ru.curs.celesta.score.Sum;
import ru.curs.celesta.score.TableElement;
import ru.curs.celesta.score.TableRef;
import ru.curs.celesta.score.VersionedElement;
import ru.curs.celesta.score.ViewColumnMeta;
import ru.curs.celesta.score.ViewColumnType;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigInteger;
import java.sql.Connection;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ru.curs.celesta.dbutils.adaptors.constants.CommonConstants.ALTER_TABLE;

import static ru.curs.celesta.dbutils.adaptors.function.SchemalessFunctions.generateSequenceTriggerName;
import static ru.curs.celesta.dbutils.adaptors.function.SchemalessFunctions.getVersionCheckTriggerName;

/**
 * Class for SQL generation of data definition of Firebird.
 */
public final class FirebirdDdlGenerator extends DdlGenerator {


    public FirebirdDdlGenerator(DBAdaptor dmlAdaptor) {
        super(dmlAdaptor);
    }


    @Override
    List<String> createSequence(SequenceElement s) {
        List<String> result = new ArrayList<>();

        String fullSequenceName = sequenceString(s.getGrain().getName(), s.getName());

        String createSql = String.format("CREATE SEQUENCE %s", fullSequenceName);

        result.add(createSql);

        if (s.getArguments().containsKey(SequenceElement.Argument.START_WITH)) {
            Long initialStartWith = (Long) s.getArguments().get(SequenceElement.Argument.START_WITH);
            Long incrementBy = (Long) s.getArguments().get(SequenceElement.Argument.INCREMENT_BY);
            Long startWith = initialStartWith - incrementBy;

            String startWithSql = String.format(
                    "ALTER SEQUENCE %s RESTART WITH %s",
                    fullSequenceName,
                    startWith
            );

            result.add(startWithSql);
        }

        String createSeqCurValueProcSql = this.createSeqCurValueProcSql(s);
        result.add(createSeqCurValueProcSql);

        String createSeqNextValueProcSql = this.createSeqNextValueProcSql(s);
        result.add(createSeqNextValueProcSql);
        result.add("COMMIT");
        return result;
    }

    @Override
    protected List<String> alterSequence(SequenceElement s) {
        List<String> result = new ArrayList<>();

        String curValueProcName = FirebirdAdaptor.sequenceCurValueProcString(s.getGrain().getName(), s.getName());
        String nextValueProcName = FirebirdAdaptor.sequenceNextValueProcString(s.getGrain().getName(), s.getName());

        String sql = String.format("DROP PROCEDURE %s", nextValueProcName);
        result.add(sql);

        sql = String.format("DROP PROCEDURE %s", curValueProcName);
        result.add(sql);

        result.add(createSeqCurValueProcSql(s));
        result.add(createSeqNextValueProcSql(s));

        return result;
    }

    private String createSeqCurValueProcSql(SequenceElement s) {
        String fullSequenceName = sequenceString(s.getGrain().getName(), s.getName());
        String curValueProcName = FirebirdAdaptor.sequenceCurValueProcString(s.getGrain().getName(), s.getName());

        Long incrementBy = (Long) s.getArguments().get(SequenceElement.Argument.INCREMENT_BY);
        Long minValue = (Long) s.getArguments().get(SequenceElement.Argument.MINVALUE);
        Long maxValue = (Long) s.getArguments().get(SequenceElement.Argument.MAXVALUE);
        Boolean isCycle = (Boolean) s.getArgument(SequenceElement.Argument.CYCLE);

        final String resultDeterminingSql;
        final String initValSql = String.format(
                "IF (:inVal IS NULL)  %n"
                        + "  THEN SELECT GEN_ID(%s, 0) FROM RDB$DATABASE INTO val;%n"
                        + "  ELSE val = inVal;",
                fullSequenceName
        );

        if (!isCycle) {
            resultDeterminingSql = String.format(
                    "%s%n"
                            + "IF (%s)%n"
                            + "    THEN val = %s;",
                    initValSql,
                    incrementBy > 0
                            ? String.format("val > %s", maxValue)
                            : String.format("val < %s", minValue),
                    incrementBy > 0
                            ? maxValue
                            : minValue
            );
        } else {
            BigInteger incrementByModulus = BigInteger.valueOf(incrementBy).abs();
            BigInteger diffBetweenMinAndMax = BigInteger.valueOf(maxValue).subtract(BigInteger.valueOf(minValue));
            BigInteger stepsForOneCycle = diffBetweenMinAndMax.divide(incrementByModulus)
                    .add(BigInteger.ONE);

            resultDeterminingSql = initValSql + "\n"
                    + String.format(
                    "currentStep = (%s) / %s + 1; %n",
                    incrementBy > 0
                            ? String.format("val - %s", minValue)
                            : String.format("%s - val", maxValue),
                    incrementByModulus
            )
                    + String.format(
                    "IF (mod(:currentStep, %s) = 1)\n"
                            + "  THEN val = %s;\n",
                    stepsForOneCycle,
                    incrementBy > 0 ? minValue : maxValue
            )
                    + String.format(
                    "ELSE IF (mod(:currentStep, %s) = 0)%n"
                            + "  THEN val = %s;%n",
                    stepsForOneCycle,
                    incrementBy > 0 ? maxValue : minValue
            )
                    + String.format(
                    "ELSE val = %s + %s * (mod(:currentStep, %s) - 1);",
                    incrementBy > 0 ? minValue : maxValue,
                    incrementBy,
                    stepsForOneCycle
            );

        }

        return String.format(
                "CREATE PROCEDURE %s (inVal integer)%n "
                        + "RETURNS (val integer)%n "
                        + "  AS%n"
                        + (isCycle ? "  declare variable currentStep integer;%n " : "")
                        + "  BEGIN%n"
                        + "  %s%n"
                        + "  END",
                curValueProcName,
                resultDeterminingSql
        );
    }

    private String createSeqNextValueProcSql(SequenceElement s) {
        String fullSequenceName = sequenceString(s.getGrain().getName(), s.getName());
        String curValueProcName = FirebirdAdaptor.sequenceCurValueProcString(s.getGrain().getName(), s.getName());
        String nextValueProcName = FirebirdAdaptor.sequenceNextValueProcString(s.getGrain().getName(), s.getName());

        Long incrementBy = (Long) s.getArguments().get(SequenceElement.Argument.INCREMENT_BY);
        Long minValue = (Long) s.getArguments().get(SequenceElement.Argument.MINVALUE);
        Long maxValue = (Long) s.getArguments().get(SequenceElement.Argument.MAXVALUE);
        Boolean isCycle = (Boolean) s.getArgument(SequenceElement.Argument.CYCLE);

        final String resultDeterminingSql;
        final String nextValueSql = String.format(
                "SELECT GEN_ID(%s, %s) FROM RDB$DATABASE INTO val;",
                fullSequenceName,
                incrementBy
        );

        if (!isCycle) {
            resultDeterminingSql = String.format(
                    "%s%n"
                            + "IF (%s)%n"
                            + "    THEN EXCEPTION SEQUENCE_OVERFLOW_ERROR;",
                    nextValueSql,
                    incrementBy > 0
                            ? String.format("val > %s", maxValue)
                            : String.format("val < %s", minValue)
            );
        } else {
            resultDeterminingSql = String.format(
                    "%s%n"
                            + "EXECUTE PROCEDURE %s(val) RETURNING_VALUES val;",
                    nextValueSql,
                    curValueProcName
            );
        }


        return String.format(
                "CREATE PROCEDURE %s%n "
                        + "RETURNS (val integer)%n "
                        + "  AS%n"
                        + (isCycle ? "  declare variable currentStep integer;%n " : "")
                        + "  BEGIN%n"
                        + "  /* INCREMENT_BY = %s, MINVALUE = %s, MAXVALUE = %s, CYCLE = %s */%n"
                        + "  %s%n"
                        + "  END",
                nextValueProcName,
                incrementBy,
                minValue,
                maxValue,
                isCycle,
                resultDeterminingSql
        );
    }

    @Override
    List<String> afterCreateTable(Connection conn, TableElement t) {
        List<String> result = new ArrayList<>();
        //creating of triggers to emulate default sequence values

        for (Column<?> column : t.getColumns().values()) {
            if (IntegerColumn.class.equals(column.getClass())) {
                IntegerColumn ic = (IntegerColumn) column;

                if (ic.getSequence() != null) {
                    final String triggerName = generateSequenceTriggerName(ic);

                    List<String> sqlList = createOrReplaceSequenceTriggerForColumn(conn, triggerName, ic);
                    result.addAll(sqlList);

                    TriggerQuery query = new TriggerQuery()
                            .withSchema(t.getGrain().getName())
                            .withTableName(t.getName())
                            .withName(triggerName);
                    this.rememberTrigger(query);
                }
            }
        }
        result.add("COMMIT");
        return result;
    }

    @Override
    List<String> dropParameterizedView(String schemaName, String viewName, Connection conn) {
        String sql = String.format(
                "DROP PROCEDURE %s",
                tableString(schemaName, viewName)
        );

        return Collections.singletonList(sql);
    }

    @Override
    List<String> dropIndex(Grain g, DbIndexInfo dBIndexInfo) {
        String sql = String.format(
                "DROP INDEX %s",
                tableString(g.getName(), dBIndexInfo.getIndexName())
        );

        return Collections.singletonList(sql);
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

        String triggerName = getVersionCheckTriggerName(t);

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
                                "CREATE TRIGGER \"" + triggerName + "\" "
                                        + "for " + tableString(t.getGrain().getName(), t.getName())
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

        return Collections.singletonList(sql);
    }

    @Override
    List<String> updateColumn(Connection conn, Column<?> c, DbColumnInfo actual) {
        @SuppressWarnings("unchecked") final Class<? extends Column<?>> cClass = (Class<Column<?>>) c.getClass();

        List<String> result = new ArrayList<>();

        final String tableFullName = tableString(c.getParentTable().getGrain().getName(), c.getParentTable().getName());

        TableElement t = c.getParentTable();
        final String triggerName = getVersionCheckTriggerName(t);

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
        if (c.getDefaultValue() != null || c instanceof DateTimeColumn && ((DateTimeColumn) c).isGetdate()) {
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
                    final String sequenceTriggerName = generateSequenceTriggerName(ic);

                    List<String> sqlList = createOrReplaceSequenceTriggerForColumn(conn, sequenceTriggerName, ic);
                    result.addAll(sqlList);

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
                            List<String> sqlList = createOrReplaceSequenceTriggerForColumn(
                                    conn,
                                    generateSequenceTriggerName(ic),
                                    ic);
                            result.addAll(sqlList);

                            TriggerQuery triggerQuery = new TriggerQuery()
                                    .withSchema(c.getParentTable().getGrain().getName())
                                    .withTableName(c.getParentTable().getName())
                                    .withName(generateSequenceTriggerName(ic))
                                    .withType(TriggerType.PRE_INSERT);

                            this.rememberTrigger(triggerQuery);
                        }
                    }
                } else if (ic.getSequence() != null) {
                    List<String> sqlList = createOrReplaceSequenceTriggerForColumn(
                            conn,
                            generateSequenceTriggerName(ic),
                            ic);
                    result.addAll(sqlList);
                }
            }
        }
        // TODO:: END COPY-PASTE
        result.add("COMMIT");
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
        Map<String, String> textParamMap = new ParameterizedViewTypeResolver<>(
                pv,
                ViewColumnType.TEXT,
                StringColumn.class,
                sc -> sc.isMax() ? 0 : sc.getLength(),
                (oldLength, newLength) -> {
                    if (oldLength == 0 || newLength == 0) {
                        return 0;
                    } else {
                        return Math.max(oldLength, newLength);
                    }
                },
                length -> {
                    if (length == 0) {
                        return "blob sub_type text";
                    } else {
                        return String.format("varchar(%d)", length);
                    }
                }
        ).resolveTypes();

        final class ScaleAndPrecision {
            private int precision;
            private int scale;

            private ScaleAndPrecision(int precision, int scale) {
                this.precision = precision;
                this.scale = scale;
            }
        }

        // Calculating of max available varchar length for input params
        Map<String, String> decimalParamMap = new ParameterizedViewTypeResolver<>(
                pv,
                ViewColumnType.DECIMAL,
                DecimalColumn.class,
                dc -> new ScaleAndPrecision(dc.getPrecision(), dc.getScale()),
                (oldValue, newValue) -> new ScaleAndPrecision(
                        Math.max(oldValue.precision, newValue.precision),
                        Math.max(oldValue.scale, newValue.scale)
                ),
                scaleAndPrecision ->
                        String.format(
                                "%s(%s,%s)",
                                ColumnDefinerFactory.getColumnDefiner(getType(), DecimalColumn.class).dbFieldType(),
                                scaleAndPrecision.precision,
                                scaleAndPrecision.scale
                        )
        ).resolveTypes();

        String inParams = pv.getParameters()
                .entrySet().stream()
                .map(e -> {
                            final String type;

                            ViewColumnType viewColumnType = e.getValue().getType();
                            if (ViewColumnType.TEXT == viewColumnType) {
                                type = textParamMap.get(e.getKey());
                            } else if (ViewColumnType.DECIMAL == viewColumnType) {
                                type = decimalParamMap.get(e.getKey());
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

                            ViewColumnMeta<?> viewColumnMeta = e.getValue();
                            if (ViewColumnType.TEXT == viewColumnMeta.getColumnType()) {
                                StringColumn sc = (StringColumn) pv.getColumnRef(viewColumnMeta.getName());

                                if (sc.isMax()) {
                                    type = "blob sub_type text";
                                } else {
                                    type = String.format("varchar(%d)", sc.getLength());
                                }
                            } else if (ViewColumnType.DECIMAL == viewColumnMeta.getColumnType()) {
                                DecimalColumn dc = (DecimalColumn) pv.getColumnRef(viewColumnMeta.getName());

                                if (dc != null) {
                                    type = String.format(
                                            "%s(%s,%s)",
                                            ColumnDefinerFactory.getColumnDefiner(getType(), DecimalColumn.class).dbFieldType(),
                                            dc.getPrecision(),
                                            dc.getScale()
                                    );
                                } else {
                                    Sum sum = (Sum) pv.getAggregateColumns().get(viewColumnMeta.getName());
                                    BinaryTermOp binaryTermOp = (BinaryTermOp) sum.getTerm();
                                    List<DecimalColumn> decimalColumns = binaryTermOp.getOperands().stream()
                                            .filter(op -> op instanceof FieldRef)
                                            .map(FieldRef.class::cast)
                                            .filter(fr -> DecimalColumn.class.equals(fr.getColumn().getClass()))
                                            .map(FieldRef::getColumn)
                                            .map(DecimalColumn.class::cast)
                                            .collect(Collectors.toList());

                                    int maxPrecision = decimalColumns.stream()
                                            .mapToInt(DecimalColumn::getPrecision)
                                            .max().getAsInt();

                                    int maxScale = decimalColumns.stream()
                                            .mapToInt(DecimalColumn::getScale)
                                            .max().getAsInt();

                                    type = String.format(
                                            "%s(%s,%s)",
                                            ColumnDefinerFactory.getColumnDefiner(getType(), DecimalColumn.class).dbFieldType(),
                                            maxPrecision,
                                            maxScale
                                    );
                                }
                            } else {
                                type = ColumnDefinerFactory.getColumnDefiner(getType(),
                                        CELESTA_TYPES_COLUMN_CLASSES.get(e.getValue().getCelestaType())
                                ).dbFieldType();
                            }

                            return String.format("\"%s\" %s", e.getKey(), type);
                        }
                ).collect(Collectors.joining(", "));

        String intoList = pv.getColumns().keySet().stream()
                .map(c -> String.format("\"%s\"", c))
                .map(":"::concat)
                .collect(Collectors.joining(", "));

        String selectSql = sw.toString();

        String sql = String.format(
                "CREATE PROCEDURE " + tableString(pv.getGrain().getName(), pv.getName()) + "(%s)%n"
                        + "  RETURNS (%s)%n"
                        + "  AS%n"
                        + "  BEGIN%n"
                        + "  FOR %s%n"
                        + "  INTO %s%n"
                        + "    DO BEGIN%n"
                        + "      SUSPEND;%n"
                        + "    END%n"
                        + "  END",
                inParams, outParams, selectSql, intoList);

        return Collections.singletonList(sql);
    }

    private static class BaseLogicValuedExprExtractor {
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

        for (MaterializedView mv : mvList) {
            String fullMvName = tableString(mv.getGrain().getName(), mv.getName());

            String insertTriggerName = mv.getTriggerName(TriggerType.POST_INSERT);
            String updateTriggerName = mv.getTriggerName(TriggerType.POST_UPDATE);
            String deleteTriggerName = mv.getTriggerName(TriggerType.POST_DELETE);

            String mvColumns = mv.getColumns().keySet().stream()
                    .filter(alias -> !MaterializedView.SURROGATE_COUNT.equals(alias))
                    .map(alias -> String.format("\"%s\"", alias))
                    .collect(Collectors.joining(", "))
                    .concat(", \"" + MaterializedView.SURROGATE_COUNT + "\"");

            String aggregateColumns = mv.getColumns().keySet().stream()
                    .filter(alias -> !MaterializedView.SURROGATE_COUNT.equals(alias))
                    .map(alias -> "\"aggregate\".\"" + alias + "\"")
                    .collect(Collectors.joining(", "))
                    .concat(", \"" + MaterializedView.SURROGATE_COUNT + "\"");

            String selectPartOfScriptTemplate = mv.getColumns().keySet().stream()
                    .filter(alias -> !MaterializedView.SURROGATE_COUNT.equals(alias))
                    .map(alias -> {
                        Column<?> colRef = mv.getColumnRef(alias);

                        Map<String, Expr> aggrCols = mv.getAggregateColumns();
                        if (aggrCols.containsKey(alias)) {
                            if (colRef == null) {
                                if (aggrCols.get(alias) instanceof Count) {
                                    return "1 as \"" + alias + "\"";
                                }
                                return "";
                            } else if (aggrCols.get(alias) instanceof Sum) {
                                return "%1$s.\"" + colRef.getName() + "\" as " + "\"" + alias + "\"";
                            } else {
                                return "";
                            }
                        }

                        if (DateTimeColumn.CELESTA_TYPE.equals(colRef.getCelestaType())) {
                            return truncDate("%1$s.\"" + colRef.getName() + "\"") + "as \"" + alias + "\"";
                        }

                        return "%1$s.\"" + colRef.getName() + "\" as " + "\"" + alias + "\"";
                    })
                    .filter(str -> !str.isEmpty())
                    .collect(Collectors.joining(", "))
                    .concat(", 1 AS \"" + MaterializedView.SURROGATE_COUNT + "\"");

            String tableGroupByColumns = mv.getColumns().values().stream()
                    .filter(v -> mv.isGroupByColumn(v.getName()))
                    .map(v -> "\"" + mv.getColumnRef(v.getName()).getName() + "\"")
                    .collect(Collectors.joining(", "));

            String rowConditionTemplate = mv.getColumns().keySet().stream()
                    .filter(mv::isGroupByColumn)
                    .map(alias -> "\"mv\".\"" + alias + "\" = \"%1$s\".\"" + alias + "\" ")
                    .collect(Collectors.joining(" AND "));

            StringBuilder insertSqlBuilder = new StringBuilder("MERGE INTO %s \"mv\" ")
                    .append("USING (SELECT %s FROM RDB$DATABASE) AS \"aggregate\" ON %s \n")
                    .append("WHEN MATCHED THEN \n ")
                    .append("UPDATE SET %s \n")
                    .append("WHEN NOT MATCHED THEN \n")
                    .append("INSERT (%s) VALUES (%s); \n");

            String setStatementTemplate = mv.getAggregateColumns().entrySet().stream()
                    .map(e -> {
                        StringBuilder sb = new StringBuilder();
                        String alias = e.getKey();

                        sb.append("\"mv\".\"").append(alias)
                                .append("\" = \"mv\".\"").append(alias)
                                .append("\" %1$s \"aggregate\".\"").append(alias).append("\"");

                        return sb.toString();
                    }).collect(Collectors.joining(", "))
                    .concat(", \"mv\".\"").concat(MaterializedView.SURROGATE_COUNT).concat("\" = ")
                    .concat("\"mv\".\"").concat(MaterializedView.SURROGATE_COUNT).concat("\" %1$s \"aggregate\".\"")
                    .concat(MaterializedView.SURROGATE_COUNT).concat("\"");

            String insertSql = String.format(insertSqlBuilder.toString(), fullMvName,
                    String.format(selectPartOfScriptTemplate, "NEW"), String.format(rowConditionTemplate, "aggregate"),
                    String.format(setStatementTemplate, "+"), mvColumns, aggregateColumns);

            String deleteMatchedCondTemplate = mv.getAggregateColumns().keySet().stream()
                    .map(alias -> "\"mv\".\"" + alias + "\" %1$s \"aggregate\".\"" + alias + "\"")
                    .collect(Collectors.joining(" %2$s "));

            String rowConditionForExistsTemplate = mv.getColumns().keySet().stream()
                    .filter(mv::isGroupByColumn)
                    .map(alias -> {
                        Column<?> colRef = mv.getColumnRef(alias);

                        if (DateTimeColumn.CELESTA_TYPE.equals(colRef.getCelestaType())) {
                            return "\"mv\".\"" + alias + "\" = "
                                    + truncDate("\"%1$s\".\"" + mv.getColumnRef(alias).getName() + "\"");
                        }

                        return "\"mv\".\"" + alias + "\" = \"%1$s\".\"" + mv.getColumnRef(alias).getName() + "\" ";
                    })
                    .collect(Collectors.joining(" AND "));

            String existsSql = "EXISTS(SELECT * FROM " + fullTableName + " AS \"t\" WHERE "
                    + String.format(rowConditionForExistsTemplate, "t") + ")";

            StringBuilder deleteSqlBuilder = new StringBuilder("MERGE INTO %s AS \"mv\" \n")
                    .append("USING (SELECT %s FROM RDB$DATABASE) AS \"aggregate\" ON %s \n")
                    .append("WHEN MATCHED AND %s THEN DELETE\n ")
                    .append("WHEN MATCHED AND (%s) THEN \n")
                    .append("UPDATE SET %s; \n");

            String deleteSql = String.format(deleteSqlBuilder.toString(), fullMvName,
                    String.format(selectPartOfScriptTemplate, "OLD"), String.format(rowConditionTemplate, "aggregate"),
                    String.format(deleteMatchedCondTemplate, "=", "AND").concat(" AND NOT " + existsSql),
                    String.format(deleteMatchedCondTemplate, "<>", "OR")
                            .concat(" OR (" + String.format(deleteMatchedCondTemplate, "=", "AND")
                                    .concat(" AND " + existsSql + ")")),
                    String.format(setStatementTemplate, "-"));

            String sql = "CREATE TRIGGER \"" + insertTriggerName + "\" "
                    + "for " + fullTableName
                    + " AFTER INSERT \n"
                    + " AS \n"
                    + " BEGIN \n"
                    + String.format(MaterializedView.CHECKSUM_COMMENT_TEMPLATE, mv.getChecksum())
                    + "\n " + insertSql + "\n END;";

            result.add(sql);

            sql = "CREATE TRIGGER \"" + deleteTriggerName + "\" "
                    + "for " + fullTableName
                    + " AFTER DELETE \n"
                    + " AS \n"
                    + " BEGIN \n"
                    + String.format(MaterializedView.CHECKSUM_COMMENT_TEMPLATE, mv.getChecksum())
                    + "\n " + deleteSql + "\n END;";

            result.add(sql);

            String updateSql = String.format("%s%n %n%s", deleteSql, insertSql);
            sql = "CREATE TRIGGER \"" + updateTriggerName + "\" "
                    + "for " + fullTableName
                    + " AFTER UPDATE \n"
                    + " AS \n"
                    + " BEGIN \n"
                    + String.format(MaterializedView.CHECKSUM_COMMENT_TEMPLATE, mv.getChecksum())
                    + "\n " + updateSql + "\n END;";

            result.add(sql);
        }

        return result;
    }

    @Override
    String truncDate(String dateStr) {
        return String.format("CAST(CAST(%s as Date) AS TIMESTAMP)", dateStr);
    }


    private List<String> createOrReplaceSequenceTriggerForColumn(Connection conn, String triggerName,
                                                                 IntegerColumn ic) {
        List<String> result = new ArrayList<>();
        TableElement te = ic.getParentTable();

        SequenceElement s = ic.getSequence();
        String nextValueProcName = FirebirdAdaptor.sequenceNextValueProcString(s.getGrain().getName(), s.getName());

        TriggerQuery triggerQuery = new TriggerQuery()
                .withSchema(ic.getParentTable().getGrain().getName())
                .withTableName(ic.getParentTable().getName())
                .withName(triggerName)
                .withType(TriggerType.PRE_INSERT);

        if (this.triggerExists(conn, triggerQuery)) {
            result.add(String.format("DROP TRIGGER \"%s\"", triggerName));
        }

        String sql =
                "CREATE TRIGGER \"" + triggerName + "\" "
                        + "for " + tableString(te.getGrain().getName(), te.getName())
                        + " BEFORE INSERT \n"
                        + " AS \n"
                        + " BEGIN \n"
                        + "   IF (NEW." + ic.getQuotedName() + " IS NULL)\n"
                        + "     THEN EXECUTE PROCEDURE " + nextValueProcName + " "
                        + "       RETURNING_VALUES :NEW." + ic.getQuotedName() + ";"
                        + " END";

        result.add(sql);

        return result;
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
                "ALTER TABLE %s%n" + " ALTER COLUMN %s TO %s",
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

    private static final class ParameterizedViewTypeResolver<T, R> {

        private final ParameterizedView pv;
        private final ViewColumnType viewColumnType;
        private final Class<R> columnClass;
        private final Function<R, T> valueResolver;
        private final BinaryOperator<T> valueMerger;
        private final Function<T, String> postMapper;


        private ParameterizedViewTypeResolver(ParameterizedView pv, ViewColumnType viewColumnType, Class<R> columnClass,
                                              Function<R, T> valueResolver, BinaryOperator<T> valueMerger,
                                              Function<T, String> postMapper) {
            this.pv = pv;
            this.viewColumnType = viewColumnType;
            this.columnClass = columnClass;
            this.valueResolver = valueResolver;
            this.valueMerger = valueMerger;
            this.postMapper = postMapper;
        }

        private Map<String, String> resolveTypes() {
            Map<String, String> columnToTypeMap =
                    pv.getSegments().stream()
                            .map(ParameterizedViewSelectStmt.class::cast)
                            .map(ParameterizedViewSelectStmt::getWhereCondition)
                            .map(LogicValuedExpr.class::cast)
                            .map(logicValuedExpr -> new BaseLogicValuedExprExtractor().extract(logicValuedExpr))
                            .flatMap(List::stream)
                            .filter(logicValuedExpr -> {
                                Set<Class<? extends Expr>> opsClasses = logicValuedExpr.getAllOperands().stream()
                                        .map(Expr::getClass)
                                        .collect(Collectors.toSet());

                                return opsClasses.containsAll(Arrays.asList(ParameterRef.class, FieldRef.class));
                            })
                            .map(logicValuedExpr -> {
                                        Map<Class<?>, List<Expr>> classToExprsMap = logicValuedExpr.getAllOperands().stream()
                                                .collect(Collectors.toMap(
                                                        Expr::getClass,
                                                        expr -> new ArrayList<>(Arrays.asList(expr)),
                                                        (oldList, newList) -> Stream.of(oldList, newList)
                                                                .flatMap(List::stream).collect(Collectors.toList())
                                                ));

                                        return classToExprsMap;
                                    }
                            ).filter(classExprMap ->
                            classExprMap.get(ParameterRef.class).stream()
                                    .anyMatch(expr -> this.viewColumnType.equals(expr.getMeta().getColumnType()))
                    )
                            .map(classExprMap -> {
                                Map<Class<? extends Expr>, List<Expr>> result = new HashMap<>();
                                result.put(
                                        ParameterRef.class,
                                        classExprMap.get(ParameterRef.class).stream()
                                                .map(ParameterRef.class::cast)
                                                .filter(parameterRef -> this.viewColumnType.equals(parameterRef.getMeta().getColumnType()))
                                                .collect(Collectors.toList())
                                );
                                result.put(
                                        FieldRef.class,
                                        classExprMap.get(FieldRef.class).stream()
                                                .map(FieldRef.class::cast)
                                                .filter(fieldRef -> fieldRef.getColumn().getClass() == columnClass)
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
                                    .map(this.columnClass::cast)
                                    .map(sc -> new AbstractMap.SimpleEntry<>(e.getKey(), sc))
                                    .collect(Collectors.toList())
                            )
                            .flatMap(List::stream)
                            .collect(
                                    Collectors.toMap(
                                            e -> e.getKey().getName(),
                                            e -> this.valueResolver.apply(e.getValue()),
                                            this.valueMerger::apply
                                    )
                            ).entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            e -> this.postMapper.apply(e.getValue())
                                    )
                            );

            return columnToTypeMap;
        }
    }

    @Override
    void processCreateUpdateRule(Connection conn, ForeignKey fk, LinkedList<StringBuilder> sqlQueue) {
        super.processCreateUpdateRule(conn, fk, sqlQueue);
        //In Firebird, FK changes should be commited
        StringBuilder sb = new StringBuilder("COMMIT");
        sqlQueue.add(sb);
    }
}

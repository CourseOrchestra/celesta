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

import java.sql.Connection;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
        return null;
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
        return null;
    }

    @Override
    public String dropPk(TableElement t, String pkName) {

        return "QWE";

        // TODO:: !!!
        /*
        return String.format(
            "ALTER TABLE %s DROP CONSTRAINT \"%s\"",
            this.tableString(t.getGrain().getName(), t.getName()),
            pkName
        );*/
    }

    @Override
    DBType getType() {
        return DBType.FIREBIRD;
    }

    @Override
    List<String> updateVersioningTrigger(Connection conn, TableElement t) {
        List<String> result = new ArrayList<>();

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

        return result;
    }

    @Override
    SQLGenerator getViewSQLGenerator() {
        return null;
    }

    @Override
    List<String> createParameterizedView(ParameterizedView pv) {
        return null;
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

            StringBuilder insertSqlBuilder = new StringBuilder("MERGE INTO %s mv")
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

            result.add(sql);
        }

        return result;
    }

    @Override
    String truncDate(String dateStr) {
        return null;
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
            result.add(alterSql.toString());
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

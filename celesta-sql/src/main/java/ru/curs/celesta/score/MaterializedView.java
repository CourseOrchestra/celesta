package ru.curs.celesta.score;

import ru.curs.celesta.event.TriggerType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Materialized view data element.
 *
 * @author ioann
 * @since 2017-06-08
 */
public final class MaterializedView extends AbstractView implements TableElement {

    /**
     * System field name that contains result of COUNT().
     */
    public static final String SURROGATE_COUNT = "surrogate_count";

    private static final String CHECKSUM_SEPARATOR = "CHECKSUM";

    /**
     * Checksum comment template.
     */
    public static final String CHECKSUM_COMMENT_TEMPLATE = "/*" + CHECKSUM_SEPARATOR + "%s" + CHECKSUM_SEPARATOR + "*/";

    private final IntegerColumn surrogateCount;

    public IntegerColumn getSurrogateCount() {
        return surrogateCount;
    }

    @FunctionalInterface
    interface MatColFabricFunction {
        Column<?> apply(MaterializedView mView, Column<?> colRef, String alias) throws ParseException;
    }

    static final Map<Class<? extends Column<?>>, MatColFabricFunction>
            COL_CLASSES_AND_FABRIC_FUNCS = new HashMap<>();

    static {
        COL_CLASSES_AND_FABRIC_FUNCS.put(IntegerColumn.class,
                (mView, colRef, alias) -> new IntegerColumn(mView, alias));
        COL_CLASSES_AND_FABRIC_FUNCS.put(FloatingColumn.class,
                (mView, colRef, alias) -> new FloatingColumn(mView, alias));
        COL_CLASSES_AND_FABRIC_FUNCS.put(
                DecimalColumn.class, (mView, colRef, alias) -> {
                    DecimalColumn dc = (DecimalColumn) colRef;
                    int precision = dc.getPrecision();
                    int scale = dc.getScale();
                    return new DecimalColumn(mView, alias, precision, scale);
                }
        );
        COL_CLASSES_AND_FABRIC_FUNCS.put(BooleanColumn.class,
                (mView, colRef, alias) -> new BooleanColumn(mView, alias));
        COL_CLASSES_AND_FABRIC_FUNCS.put(BinaryColumn.class,
                (mView, colRef, alias) -> new BinaryColumn(mView, alias));
        COL_CLASSES_AND_FABRIC_FUNCS.put(DateTimeColumn.class,
                (mView, colRef, alias) -> new DateTimeColumn(mView, alias));
        COL_CLASSES_AND_FABRIC_FUNCS.put(ZonedDateTimeColumn.class,
                (mView, colRef, alias) -> new ZonedDateTimeColumn(mView, alias));
        COL_CLASSES_AND_FABRIC_FUNCS.put(StringColumn.class, (mView, colRef, alias) -> {
            StringColumn result = new StringColumn(mView, alias);
            StringColumn strColRef = (StringColumn) colRef;
            result.setLength(String.valueOf(strColRef.getLength()));
            return result;
        });
    }

    private final NamedElementHolder<Column<?>> realColumns = new NamedElementHolder<Column<?>>() {
        @Override
        protected String getErrorMsg(String name) {
            return String.format("Column '%s' defined more than once in table '%s'.", name, getName());
        }
    };

    final NamedElementHolder<Column<?>> pk = new NamedElementHolder<Column<?>>() {
        @Override
        protected String getErrorMsg(String name) {
            return String.format("Column '%s' defined more than once for primary key in table '%s'.", name, getName());
        }
    };

    public MaterializedView(GrainPart grainPart, String name) throws ParseException {
        super(grainPart, name);
        getGrain().addElement(this);
        surrogateCount = new IntegerColumn(this, SURROGATE_COUNT);
        surrogateCount.setNullableAndDefault(false, "0");
    }

    @Override
    String viewType() {
        return "materialized view";
    }

    @Override
    AbstractSelectStmt newSelectStatement() {
        return new MaterializedSelectStmt(this);
    }

    @Override
    public Map<String, Column<?>> getColumns() {
        return realColumns.getElements();
    }

    public List<String> getColumnRefNames() {
        if (getSegments().size() > 0) {
            List<String> result = new ArrayList<>();

            for (Map.Entry<String, Expr> entry : getSegments().get(0).columns.entrySet()) {
                Expr expr = entry.getValue();

                if (!(expr instanceof Count)) {
                    Column<?> colRef = EXPR_CLASSES_AND_COLUMN_EXTRACTORS.get(expr.getClass()).apply(expr);
                    result.add(colRef.getName());
                }
            }
            return result;
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public Column<?> getColumn(String colName) throws ParseException {
        Column<?> result = realColumns.get(colName);
        if (result == null) {
            throw new ParseException(
                    String.format("Column '%s' not found in materialized view '%s.%s'",
                            colName, getGrain().getName(), getName()));
        }

        return result;
    }

    @Override
    public void addColumn(Column<?> column) throws ParseException {
        if (column.getParentTable() != this) {
            throw new IllegalArgumentException();
        }
        getGrain().modify();
        realColumns.addElement(column);
    }

    @Override
    public synchronized void removeColumn(Column<?> column) throws ParseException {
        // It's not allowed to delete compound part of the primary key
        if (pk.contains(column)) {
            throw new ParseException(
                    String.format(YOU_CANNOT_DROP_A_COLUMN_THAT_BELONGS_TO + "a primary key. Change primary key first.",
                            getGrain().getName(), getName(), column.getName()));
        }
        // It's not allowed to delete compound part of an index
        for (Index ind : getGrain().getIndices().values()) {
            if (ind.getColumns().containsValue(column)) {
                throw new ParseException(String.format(
                        YOU_CANNOT_DROP_A_COLUMN_THAT_BELONGS_TO + "an index. Drop or change relevant index first.",
                        getGrain().getName(), getName(), column.getName()));
            }
        }

        getGrain().modify();
        realColumns.remove(column);
    }

    @Override
    public boolean hasPrimeKey() {
        return !pk.getElements().isEmpty();
    }

    @Override
    public String getPkConstraintName() {
        return limitName("pk_" + getName());
    }

    @Override
    public Map<String, Column<?>> getPrimaryKey() {
        return pk.getElements();
    }

    public TableRef getRefTable() {
        return getSegments().get(0).tables.values().stream().findFirst().get();
    }

    public boolean isGroupByColumn(String alias) {
        return getSegments().get(0).groupByColumns.containsKey(alias);
    }

    public String getSelectPartOfScript() {
        try {
            SQLGenerator gen = new SQLGenerator();
            StringWriter sw = new StringWriter();
            PrintWriter bw = new PrintWriter(sw);
            BWWrapper bww = new BWWrapper();

            getSegments().get(0).writeSelectPart(bw, gen, bww);
            bw.flush();
            return sw.getBuffer().toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getGroupByPartOfScript() {
        try {
            SQLGenerator gen = new SQLGenerator();
            StringWriter sw = new StringWriter();
            PrintWriter bw = new PrintWriter(sw);

            getSegments().get(0).writeGroupByPart(bw, gen);
            bw.flush();
            return sw.getBuffer().toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public String getChecksum() {
        // TODO: CelestaSerializer is not intended to be used from GrainElement classes.
        //       Consider using a different approach for checksum calculation.
        try (ChecksumInputStream is = new ChecksumInputStream(
                new ByteArrayInputStream(CelestaSerializer.toString(this).getBytes(StandardCharsets.UTF_8))
        )) {
            while (is.read() != -1) ;
            return String.format("%08X", is.getCRC32());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public String getTriggerName(TriggerType type) {
        BasicTable t = getRefTable().getTable();

        TriggerNameBuilder tnb = new TriggerNameBuilder()
                .withSchema(getGrain().getName())
                .withTableName(t.getName())
                .withName(getName())
                .withType(type);

        return tnb.build();
    }

    static final class TriggerNameBuilder {
        private static final Map<TriggerType, String> TRIGGER_TYPES_TO_NAME_PARTS = new HashMap<>();
        private static final String TEMPLATE = "mv%sFrom%s_%sTo%s_%s";

        static {
            TRIGGER_TYPES_TO_NAME_PARTS.put(TriggerType.POST_INSERT, "Insert");
            TRIGGER_TYPES_TO_NAME_PARTS.put(TriggerType.POST_UPDATE, "Update");
            TRIGGER_TYPES_TO_NAME_PARTS.put(TriggerType.POST_DELETE, "Delete");
        }

        private String schema;
        private String tableName;
        private String name;
        private TriggerType type;

        public TriggerNameBuilder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public TriggerNameBuilder withSchema(String schema) {
            this.schema = schema;
            return this;
        }

        public TriggerNameBuilder withName(String name) {
            this.name = name;
            return this;
        }

        public TriggerNameBuilder withType(TriggerType type) {
            this.type = type;
            return this;
        }

        public String build() {
            String preResult = String.format(TEMPLATE, TRIGGER_TYPES_TO_NAME_PARTS.get(type),
                    schema, tableName, schema, name);
            return limitName(preResult);
        }
    }
}

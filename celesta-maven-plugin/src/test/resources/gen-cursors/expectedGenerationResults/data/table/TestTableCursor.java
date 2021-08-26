package data.table;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Consumer;
import javax.annotation.Generated;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.ICelesta;
import ru.curs.celesta.dbutils.BLOB;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.CelestaGenerated;
import ru.curs.celesta.dbutils.Cursor;
import ru.curs.celesta.dbutils.CursorIterator;
import ru.curs.celesta.event.TriggerType;
import ru.curs.celesta.score.ColumnMeta;
import ru.curs.celesta.score.Table;

@Generated(
        value = "ru.curs.celesta.plugin.maven.CursorGenerator",
        date = "2021-04-15T01:58:49.563"
)
@CelestaGenerated
public class TestTableCursor extends Cursor implements Iterable<TestTableCursor>, Serializable, Cloneable {
    private static final String GRAIN_NAME = "test";

    private static final String OBJECT_NAME = "testTable";

    public final TestTableCursor.Columns COLUMNS;

    private Integer id;

    private String str;

    private Boolean deleted;

    private Double weight;

    private String content;

    private Date created;

    private BLOB rawData;

    private BigDecimal cost;

    private ZonedDateTime toDelete;

    {
        this.COLUMNS = new TestTableCursor.Columns(callContext().getCelesta());
    }

    public TestTableCursor(CallContext context) {
        super(context);
    }

    public TestTableCursor(CallContext context, ColumnMeta<?>... columns) {
        super(context, columns);
    }

    @Deprecated
    public TestTableCursor(CallContext context, Set<String> fields) {
        super(context, fields);
    }

    public Integer getId() {
        return this.id;
    }

    public TestTableCursor setId(Integer id) {
        this.id = id;
        return this;
    }

    public String getStr() {
        return this.str;
    }

    public TestTableCursor setStr(String str) {
        this.str = str;
        return this;
    }

    public Boolean getDeleted() {
        return this.deleted;
    }

    public TestTableCursor setDeleted(Boolean deleted) {
        this.deleted = deleted;
        return this;
    }

    public Double getWeight() {
        return this.weight;
    }

    public TestTableCursor setWeight(Double weight) {
        this.weight = weight;
        return this;
    }

    public String getContent() {
        return this.content;
    }

    public TestTableCursor setContent(String content) {
        this.content = content;
        return this;
    }

    public Date getCreated() {
        return this.created;
    }

    public TestTableCursor setCreated(Date created) {
        this.created = created;
        return this;
    }

    public BLOB getRawData() {
        return this.rawData;
    }

    public TestTableCursor setRawData(BLOB rawData) {
        this.rawData = rawData;
        return this;
    }

    public BigDecimal getCost() {
        return this.cost;
    }

    public TestTableCursor setCost(BigDecimal cost) {
        this.cost = cost;
        return this;
    }

    public ZonedDateTime getToDelete() {
        return this.toDelete;
    }

    public TestTableCursor setToDelete(ZonedDateTime toDelete) {
        this.toDelete = toDelete;
        return this;
    }

    @Override
    protected Object _getFieldValue(String name) {
        try {
            Field f = getClass().getDeclaredField(name);
            f.setAccessible(true);
            return f.get(this);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void _setFieldValue(String name, Object value) {
        try {
            Field f = getClass().getDeclaredField(name);
            f.setAccessible(true);
            f.set(this, value);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Object[] _currentKeyValues() {
        return new Object[] {id};
    }

    public boolean tryGet(Integer id) {
        return tryGetByValuesArray(id);
    }

    public void get(Integer id) {
        getByValuesArray(id);
    }

    @Override
    protected void _parseResultInternal(ResultSet rs) throws SQLException {
        if (this.inRec("id")) {
            this.id = rs.getInt("id");
            if (rs.wasNull()) {
                this.id = null;
            }
        }
        if (this.inRec("str")) {
            this.str = rs.getString("str");
            if (rs.wasNull()) {
                this.str = null;
            }
        }
        if (this.inRec("deleted")) {
            this.deleted = rs.getBoolean("deleted");
            if (rs.wasNull()) {
                this.deleted = null;
            }
        }
        if (this.inRec("weight")) {
            this.weight = rs.getDouble("weight");
            if (rs.wasNull()) {
                this.weight = null;
            }
        }
        if (this.inRec("content")) {
            this.content = rs.getString("content");
            if (rs.wasNull()) {
                this.content = null;
            }
        }
        if (this.inRec("created")) {
            this.created = rs.getTimestamp("created");
            if (rs.wasNull()) {
                this.created = null;
            }
        }
        this.rawData = null;
        if (this.inRec("cost")) {
            this.cost = rs.getBigDecimal("cost");
            if (rs.wasNull()) {
                this.cost = null;
            }
        }
        if (this.inRec("toDelete")) {
            Timestamp ts = rs.getTimestamp("toDelete", Calendar.getInstance(TimeZone.getTimeZone("UTC")));
            if (ts != null) {
                this.toDelete = ZonedDateTime.of(ts.toLocalDateTime(), ZoneOffset.systemDefault());
            }
            else {
                this.toDelete = null;
            }
        }
        this.setRecversion(rs.getInt("recversion"));
    }

    @Override
    public void _clearBuffer(boolean withKeys) {
        if (withKeys) {
            this.id = null;
        }
        this.str = null;
        this.deleted = null;
        this.weight = null;
        this.content = null;
        this.created = null;
        this.rawData = null;
        this.cost = null;
        this.toDelete = null;
    }

    @Override
    public Object[] _currentValues() {
        return new Object[] {id, str, deleted, weight, content, created, rawData, cost, toDelete};
    }

    public void calcRawData() {
        this.rawData = this.calcBlob("rawData");
        ((TestTableCursor)this.getXRec()).rawData = this.rawData.clone();
    }

    @Override
    protected void _setAutoIncrement(int val) {
        this.id = val;
    }

    public static void onPreDelete(ICelesta celesta,
            Consumer<? super TestTableCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.PRE_DELETE, TestTableCursor.class, cursorConsumer);
    }

    public static void onPostDelete(ICelesta celesta,
            Consumer<? super TestTableCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.POST_DELETE, TestTableCursor.class, cursorConsumer);
    }

    public static void onPreInsert(ICelesta celesta,
            Consumer<? super TestTableCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.PRE_INSERT, TestTableCursor.class, cursorConsumer);
    }

    public static void onPostInsert(ICelesta celesta,
            Consumer<? super TestTableCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.POST_INSERT, TestTableCursor.class, cursorConsumer);
    }

    public static void onPreUpdate(ICelesta celesta,
            Consumer<? super TestTableCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.PRE_UPDATE, TestTableCursor.class, cursorConsumer);
    }

    public static void onPostUpdate(ICelesta celesta,
            Consumer<? super TestTableCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.POST_UPDATE, TestTableCursor.class, cursorConsumer);
    }

    @Override
    public TestTableCursor _getBufferCopy(CallContext context, List<String> fields) {
        final TestTableCursor result;
        if (Objects.isNull(fields)) {
            result = new TestTableCursor(context);
        }
        else {
            result = new TestTableCursor(context, new LinkedHashSet<>(fields));
        }
        result.copyFieldsFrom(this);
        return result;
    }

    @Override
    public void copyFieldsFrom(BasicCursor c) {
        TestTableCursor from = (TestTableCursor)c;
        this.id = from.id;
        this.str = from.str;
        this.deleted = from.deleted;
        this.weight = from.weight;
        this.content = from.content;
        this.created = from.created;
        this.rawData = from.rawData;
        this.cost = from.cost;
        this.toDelete = from.toDelete;
        this.setRecversion(from.getRecversion());
    }

    @Override
    public Iterator<TestTableCursor> iterator() {
        return new CursorIterator<TestTableCursor>(this);
    }

    @Override
    protected String _grainName() {
        return GRAIN_NAME;
    }

    @Override
    protected String _objectName() {
        return OBJECT_NAME;
    }

    @SuppressWarnings("unchecked")
    @Generated(
            value = "ru.curs.celesta.plugin.maven.CursorGenerator",
            date = "2021-04-15T01:58:49.593"
    )
    @CelestaGenerated
    public static final class Columns {
        private final Table element;

        public Columns(ICelesta celesta) {
            this.element = celesta.getScore().getGrains().get(GRAIN_NAME).getElements(Table.class).get(OBJECT_NAME);
        }

        public ColumnMeta<Integer> id() {
            return (ColumnMeta<Integer>) this.element.getColumns().get("id");
        }

        public ColumnMeta<String> str() {
            return (ColumnMeta<String>) this.element.getColumns().get("str");
        }

        public ColumnMeta<Boolean> deleted() {
            return (ColumnMeta<Boolean>) this.element.getColumns().get("deleted");
        }

        public ColumnMeta<Double> weight() {
            return (ColumnMeta<Double>) this.element.getColumns().get("weight");
        }

        public ColumnMeta<String> content() {
            return (ColumnMeta<String>) this.element.getColumns().get("content");
        }

        public ColumnMeta<Date> created() {
            return (ColumnMeta<Date>) this.element.getColumns().get("created");
        }

        public ColumnMeta<BigDecimal> cost() {
            return (ColumnMeta<BigDecimal>) this.element.getColumns().get("cost");
        }

        public ColumnMeta<ZonedDateTime> toDelete() {
            return (ColumnMeta<ZonedDateTime>) this.element.getColumns().get("toDelete");
        }
    }

    @Generated(
            value = "ru.curs.celesta.plugin.maven.CursorGenerator",
            date = "2021-04-15T01:58:49.603"
    )
    @CelestaGenerated
    public static final class Str {
        public static final String one = "one";

        public static final String two = "two";

        public static final String three = "three";

        private Str() {
            throw new AssertionError();
        }
    }
}

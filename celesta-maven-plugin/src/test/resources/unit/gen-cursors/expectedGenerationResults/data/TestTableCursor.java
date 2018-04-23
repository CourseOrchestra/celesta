package ru.curs.celesta.jcursor.score.data;

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

import ru.curs.celesta.CallContext;
import ru.curs.celesta.ICelesta;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.Cursor;
import ru.curs.celesta.dbutils.CursorIterator;
import ru.curs.celesta.event.TriggerType;

public final class TestTableCursor extends Cursor implements Iterable<TestTableCursor>, Serializable, Cloneable {

    private Integer id;
    private String str;
    private Boolean deleted;
    private Double weight;
    private String content;
    private Date created;
    private String rawData;
    private BigDecimal cost;
    private ZonedDateTime toDelete;

    public TestTableCursor(CallContext context) {
        super(context);
    }

    public TestTableCursor(CallContext context, Set<String> fields) {
        super(context, fields);
    }

    public Integer getId() {
        return this.id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getStr() {
        return this.str;
    }

    public void setStr(String str) {
        this.str = str;
    }

    public Boolean getDeleted() {
        return this.deleted;
    }

    public void setDeleted(Boolean deleted) {
        this.deleted = deleted;
    }

    public Double getWeight() {
        return this.weight;
    }

    public void setWeight(Double weight) {
        this.weight = weight;
    }

    public String getContent() {
        return this.content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public Date getCreated() {
        return this.created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public String getRawData() {
        return this.rawData;
    }

    public void setRawData(String rawData) {
        this.rawData = rawData;
    }

    public BigDecimal getCost() {
        return this.cost;
    }

    public void setCost(BigDecimal cost) {
        this.cost = cost;
    }

    public ZonedDateTime getToDelete() {
        return this.toDelete;
    }

    public void setToDelete(ZonedDateTime toDelete) {
        this.toDelete = toDelete;
    }

    @Override
    protected String _grainName() {
        return "test";
    }

    @Override
    protected String _objectName() {
        return "testTable";
    }

    @Override
    protected void _setFieldValue(String name, Object value) {
        try {
            Field f = getClass().getDeclaredField(name);

            f.setAccessible(true);
            f.set(this, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Object[] _currentKeyValues() {
        Object[] result = new Object[1];
        result[0] = this.id;
        return result;
    }

    @Override
    protected void _parseResultInternal(ResultSet rs) throws SQLException {
        if (this.inRec("id")) {
            this.id = rs.getInt("id");
        }
        if (this.inRec("str")) {
            this.str = rs.getString("str");
        }
        if (this.inRec("deleted")) {
            this.deleted = rs.getBoolean("deleted");
        }
        if (this.inRec("weight")) {
            this.weight = rs.getDouble("weight");
        }
        if (this.inRec("content")) {
            this.content = rs.getString("content");
        }
        if (this.inRec("created")) {
            this.created = rs.getTimestamp("created");
        }
        this.rawData = null;
        if (this.inRec("cost")) {
            this.cost = rs.getBigDecimal("cost");
        }
        if (this.inRec("toDelete")) {
            Timestamp ts = rs.getTimestamp("toDelete", Calendar.getInstance(TimeZone.getTimeZone("UTC")));
            if (ts != null) {
                this.toDelete = ZonedDateTime.of(ts.toLocalDateTime(), ZoneOffset.systemDefault());
            } else {
                this.toDelete = null;
            }
        }
        this.setRecversion(rs.getInt("recversion"));
    }

    @Override
    protected void _clearBuffer(boolean withKeys) {
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
        Object[] result = new Object[9];
        result[0] = this.id;
        result[1] = this.str;
        result[2] = this.deleted;
        result[3] = this.weight;
        result[4] = this.content;
        result[5] = this.created;
        result[6] = this.rawData;
        result[7] = this.cost;
        result[8] = this.toDelete;
        return result;
    }

    public void calcRawData() {
        this.rawData = this.calcBlob("rawData");
        ((TestTableCursor)this.getXRec()).rawData = this.rawData.clone();
    }


    @Override
    protected void _setAutoIncrement(int val) {
    }


    public static void onPreDelete(ICelesta celesta, Consumer<TestTableCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.PRE_DELETE, TestTableCursor.class, cursorConsumer);
    }

    public static void onPostDelete(ICelesta celesta, Consumer<TestTableCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.POST_DELETE, TestTableCursor.class, cursorConsumer);
    }

    public static void onPreInsert(ICelesta celesta, Consumer<TestTableCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.PRE_INSERT, TestTableCursor.class, cursorConsumer);
    }

    public static void onPostInsert(ICelesta celesta, Consumer<TestTableCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.POST_INSERT, TestTableCursor.class, cursorConsumer);
    }

    public static void onPreUpdate(ICelesta celesta, Consumer<TestTableCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.PRE_UPDATE, TestTableCursor.class, cursorConsumer);
    }

    public static void onPostUpdate(ICelesta celesta, Consumer<TestTableCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.POST_UPDATE, TestTableCursor.class, cursorConsumer);
    }

    @Override
    public TestTableCursor _getBufferCopy(CallContext context, List<String> fields) {
        final TestTableCursor result;

        if (Objects.isNull(fields)) {
            result = new TestTableCursor(context);
        } else {
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

    public static final class Str {
        public static final String one = "one";
        public static final String two = "two";
        public static final String three = "three";

        private Str() {
            throw new AssertionError();
        }
    }

}

package ru.curs.celesta.jcursor.score.data;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ICelesta;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.Cursor;
import ru.curs.celesta.dbutils.CursorIterator;
import ru.curs.celesta.event.TriggerType;

public final class TestTableWithIdentityCursor extends Cursor implements Iterable<TestTableWithIdentityCursor> {

    private Integer identityId;

    public TestTableWithIdentityCursor(CallContext context) throws CelestaException {
        super(context);
    }

    public TestTableWithIdentityCursor(CallContext context, Set<String> fields) throws CelestaException {
        super(context, fields);
    }

    public Integer getIdentityId() {
        return this.identityId;
    }

    public void setIdentityId(Integer identityId) {
        this.identityId = identityId;
    }

    @Override
    protected String _grainName() {
        return "test";
    }

    @Override
    protected String _objectName() {
        return "testTableWithIdentity";
    }

    @Override
    protected void _parseResult(ResultSet rs) throws SQLException {
        if (this.inRec("identityId")) {
            this.identityId = rs.getInt("identityId");
        }
        this.setRecversion(rs.getInt("recversion"));
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
        result[0] = this.identityId;
        return result;
    }

    @Override
    protected void _clearBuffer(boolean withKeys) {
        if (withKeys) {
            this.identityId = null;
        }
    }

    @Override
    public Object[] _currentValues() {
        Object[] result = new Object[1];
        result[0] = this.identityId;
        return result;
    }

    @Override
    protected void _setAutoIncrement(int val) {
        this.identityId = val;
    }

    public static void onPreDelete(ICelesta celesta, Consumer<TestTableWithIdentityCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.PRE_DELETE, TestTableWithIdentityCursor.class, cursorConsumer);
    }

    public static void onPostDelete(ICelesta celesta, Consumer<TestTableWithIdentityCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.POST_DELETE, TestTableWithIdentityCursor.class, cursorConsumer);
    }

    public static void onPreInsert(ICelesta celesta, Consumer<TestTableWithIdentityCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.PRE_INSERT, TestTableWithIdentityCursor.class, cursorConsumer);
    }

    public static void onPostInsert(ICelesta celesta, Consumer<TestTableWithIdentityCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.POST_INSERT, TestTableWithIdentityCursor.class, cursorConsumer);
    }

    public static void onPreUpdate(ICelesta celesta, Consumer<TestTableWithIdentityCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.PRE_UPDATE, TestTableWithIdentityCursor.class, cursorConsumer);
    }

    public static void onPostUpdate(ICelesta celesta, Consumer<TestTableWithIdentityCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.POST_UPDATE, TestTableWithIdentityCursor.class, cursorConsumer);
    }

    @Override
    public TestTableWithIdentityCursor _getBufferCopy(CallContext context, List<String> fields) throws CelestaException {
        final TestTableWithIdentityCursor result;

        if (Objects.isNull(fields)) {
            result = new TestTableWithIdentityCursor(context);
        } else {
            result = new TestTableWithIdentityCursor(context, new LinkedHashSet<>(fields));
        }
        result.copyFieldsFrom(this);
        return result;
    }

    @Override
    public void copyFieldsFrom(BasicCursor c) {
        TestTableWithIdentityCursor from = (TestTableWithIdentityCursor)c;
        this.identityId = from.identityId;
        this.setRecversion(from.getRecversion());
    }

    @Override
    public Iterator<TestTableWithIdentityCursor> iterator() {
        return new CursorIterator<TestTableWithIdentityCursor>(this);
    }
}
package data.table;

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
import ru.curs.celesta.ICelesta;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.Cursor;
import ru.curs.celesta.dbutils.CursorIterator;
import ru.curs.celesta.event.TriggerType;
import ru.curs.celesta.score.ColumnMeta;
import ru.curs.celesta.score.Table;

public final class TestTableWithIdentityCursor extends Cursor implements Iterable<TestTableWithIdentityCursor> {

    private static final String GRAIN_NAME = "test";
    private static final String OBJECT_NAME = "testTableWithIdentity";

    public final TestTableWithIdentityCursor.TestTableWithIdentityCursorColumns COLUMNS = new TestTableWithIdentityCursor.TestTableWithIdentityCursorColumns();

    private Integer identityId;

    {
        this.COLUMNS = new TestTableWithIdentityCursor.Columns(callContext().getCelesta());
    }

    public TestTableWithIdentityCursor(CallContext context) {
        super(context);
    }

    public TestTableWithIdentityCursor(CallContext context, ColumnMeta<?>... columns) {
        super(context, columns);
    }

    @Deprecated
    public TestTableWithIdentityCursor(CallContext context, Set<String> fields) {
        super(context, fields);
    }

    public Integer getIdentityId() {
        return this.identityId;
    }

    public void setIdentityId(Integer identityId) {
        this.identityId = identityId;
    }

    @Override
    protected Object _getFieldValue(String name) {
        try {
            Field f = getClass().getDeclaredField(name);

            f.setAccessible(true);
            return f.get(this);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
    protected void _parseResultInternal(ResultSet rs) throws SQLException {
        if (this.inRec("identityId")) {
            this.identityId = rs.getInt("identityId");
            if (rs.wasNull()) {
                this.identityId = null;
            }
        }
        this.setRecversion(rs.getInt("recversion"));
    }

    @Override
    public void _clearBuffer(boolean withKeys) {
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

    public static void onPreDelete(ICelesta celesta, Consumer<? super TestTableWithIdentityCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.PRE_DELETE, TestTableWithIdentityCursor.class, cursorConsumer);
    }

    public static void onPostDelete(ICelesta celesta, Consumer<? super TestTableWithIdentityCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.POST_DELETE, TestTableWithIdentityCursor.class, cursorConsumer);
    }

    public static void onPreInsert(ICelesta celesta, Consumer<? super TestTableWithIdentityCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.PRE_INSERT, TestTableWithIdentityCursor.class, cursorConsumer);
    }

    public static void onPostInsert(ICelesta celesta, Consumer<? super TestTableWithIdentityCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.POST_INSERT, TestTableWithIdentityCursor.class, cursorConsumer);
    }

    public static void onPreUpdate(ICelesta celesta, Consumer<? super TestTableWithIdentityCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.PRE_UPDATE, TestTableWithIdentityCursor.class, cursorConsumer);
    }

    public static void onPostUpdate(ICelesta celesta, Consumer<? super TestTableWithIdentityCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.POST_UPDATE, TestTableWithIdentityCursor.class, cursorConsumer);
    }

    @Override
    public TestTableWithIdentityCursor _getBufferCopy(CallContext context, List<String> fields) {
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

    @Override
    protected String _grainName() {
        return GRAIN_NAME;
    }

    @Override
    protected String _objectName() {
        return OBJECT_NAME;
    }

    @SuppressWarnings("unchecked")
    public static final class Columns {
        private final Table element;

        public Columns(ICelesta celesta) {
            this.element = celesta.getScore().getGrains().get(GRAIN_NAME).getElements(Table.class).get(OBJECT_NAME);
        }

        public ColumnMeta<Integer> identityId() {
            return (ColumnMeta<Integer>) this.element.getColumns().get("identityId");
        }
    }

}

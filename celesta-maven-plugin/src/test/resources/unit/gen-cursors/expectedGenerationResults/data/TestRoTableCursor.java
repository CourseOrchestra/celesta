package ru.curs.celesta.jcursor.score.data;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.CursorIterator;
import ru.curs.celesta.dbutils.ReadOnlyTableCursor;

public final class TestRoTableCursor extends ReadOnlyTableCursor implements Iterable<TestRoTableCursor> {

    private Integer id;

    public TestRoTableCursor(CallContext context) {
        super(context);
    }

    public TestRoTableCursor(CallContext context, Set<String> fields) {
        super(context, fields);
    }

    public Integer getId() {
        return this.id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    @Override
    protected String _grainName() {
        return "test";
    }

    @Override
    protected String _objectName() {
        return "testRoTable";
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
    protected void _parseResult(ResultSet rs) throws SQLException {
        if (this.inRec("id")) {
            this.id = rs.getInt("id");
        }
    }

    @Override
    protected void _clearBuffer(boolean withKeys) {
        this.id = null;
    }

    @Override
    public Object[] _currentValues() {
        Object[] result = new Object[1];
        result[0] = this.id;
        return result;
    }

    @Override
    public TestRoTableCursor _getBufferCopy(CallContext context, List<String> fields) {
        final TestRoTableCursor result;

        if (Objects.isNull(fields)) {
            result = new TestRoTableCursor(context);
        } else {
            result = new TestRoTableCursor(context, new LinkedHashSet<>(fields));
        }
        result.copyFieldsFrom(this);
        return result;
    }

    @Override
    public void copyFieldsFrom(BasicCursor c) {
        TestRoTableCursor from = (TestRoTableCursor)c;
        this.id = from.id;
    }

    @Override
    public Iterator<TestRoTableCursor> iterator() {
        return new CursorIterator<TestRoTableCursor>(this);
    }

    public static final class Id {
        public static final Integer open = 0;
        public static final Integer closed = 1;

        private Id() {
            throw new AssertionError();
        }
    }

}

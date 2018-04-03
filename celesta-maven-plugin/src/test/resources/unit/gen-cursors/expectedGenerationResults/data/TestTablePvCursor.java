package ru.curs.celesta.jcursor.score.data;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.CursorIterator;
import ru.curs.celesta.dbutils.ParameterizedViewCursor;

public final class TestTablePvCursor extends ParameterizedViewCursor implements Iterable<TestTablePvCursor> {

    private Integer s;

    public TestTablePvCursor(CallContext context, Map<String, Object> parameters) throws CelestaException {
        super(context, parameters);
    }

    public TestTablePvCursor(CallContext context, Set<String> fields, Map<String, Object> parameters) throws CelestaException {
        super(context, fields, parameters);
    }

    public Integer getS() {
        return this.s;
    }

    public void setS(Integer s) {
        this.s = s;
    }

    @Override
    protected String _grainName() {
        return "test";
    }

    @Override
    protected String _objectName() {
        return "testTablePv";
    }

    @Override
    protected void _parseResult(ResultSet rs) throws SQLException {
        if (this.inRec("s")) {
            this.s = rs.getInt("s");
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
    protected void _clearBuffer(boolean withKeys) {
        this.s = null;
    }

    @Override
    public Object[] _currentValues() {
        Object[] result = new Object[1];
        result[0] = this.s;
        return result;
    }

    @Override
    public TestTablePvCursor _getBufferCopy(CallContext context, List<String> fields) throws CelestaException {
        final TestTablePvCursor result;

        if (Objects.isNull(fields)) {
            result = new TestTablePvCursor(context, this.parameters);
        } else {
            result = new TestTablePvCursor(context, new LinkedHashSet<>(fields), this.parameters);
        }
        result.copyFieldsFrom(this);
        return result;
    }

    @Override
    public void copyFieldsFrom(BasicCursor c) {
        TestTablePvCursor from = (TestTablePvCursor)c;
        this.s = from.s;
    }

    @Override
    public Iterator<TestTablePvCursor> iterator() {
        return new CursorIterator<TestTablePvCursor>(this);
    }
}

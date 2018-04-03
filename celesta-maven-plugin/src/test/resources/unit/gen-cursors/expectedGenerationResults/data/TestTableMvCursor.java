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
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.CursorIterator;
import ru.curs.celesta.dbutils.MaterializedViewCursor;

public final class TestTableMvCursor extends MaterializedViewCursor implements Iterable<TestTableMvCursor> {

    private Integer surrogate_count;
    private Integer c;

    public TestTableMvCursor(CallContext context) throws CelestaException {
        super(context);
    }

    public TestTableMvCursor(CallContext context, Set<String> fields) throws CelestaException {
        super(context, fields);
    }

    public Integer getSurrogate_count() {
        return this.surrogate_count;
    }

    public void setSurrogate_count(Integer surrogate_count) {
        this.surrogate_count = surrogate_count;
    }

    public Integer getC() {
        return this.c;
    }

    public void setC(Integer c) {
        this.c = c;
    }

    @Override
    protected String _grainName() {
        return "test";
    }

    @Override
    protected String _objectName() {
        return "testTableMv";
    }

    @Override
    protected void _parseResult(ResultSet rs) throws SQLException {
        if (this.inRec("surrogate_count")) {
            this.surrogate_count = rs.getInt("surrogate_count");
        }
        if (this.inRec("c")) {
            this.c = rs.getInt("c");
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
        this.surrogate_count = null;
        this.c = null;
    }

    @Override
    public Object[] _currentValues() {
        Object[] result = new Object[2];
        result[0] = this.surrogate_count;
        result[1] = this.c;
        return result;
    }

    @Override
    public TestTableMvCursor _getBufferCopy(CallContext context, List<String> fields) throws CelestaException {
        final TestTableMvCursor result;

        if (Objects.isNull(fields)) {
            result = new TestTableMvCursor(context);
        } else {
            result = new TestTableMvCursor(context, new LinkedHashSet<>(fields));
        }
        result.copyFieldsFrom(this);
        return result;
    }

    @Override
    public void copyFieldsFrom(BasicCursor c) {
        TestTableMvCursor from = (TestTableMvCursor)c;
        this.surrogate_count = from.surrogate_count;
        this.c = from.c;
    }

    @Override
    public Iterator<TestTableMvCursor> iterator() {
        return new CursorIterator<TestTableMvCursor>(this);
    }
}

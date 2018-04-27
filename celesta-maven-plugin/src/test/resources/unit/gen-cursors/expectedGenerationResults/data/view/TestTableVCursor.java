package data.view;

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
import ru.curs.celesta.dbutils.ViewCursor;

public final class TestTableVCursor extends ViewCursor implements Iterable<TestTableVCursor> {

    private Integer id;

    public TestTableVCursor(CallContext context) {
        super(context);
    }

    public TestTableVCursor(CallContext context, Set<String> fields) {
        super(context, fields);
    }

    public Integer getId() {
        return this.id;
    }

    public void setId(Integer id) {
        this.id = id;
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
    public TestTableVCursor _getBufferCopy(CallContext context, List<String> fields) {
        final TestTableVCursor result;

        if (Objects.isNull(fields)) {
            result = new TestTableVCursor(context);
        } else {
            result = new TestTableVCursor(context, new LinkedHashSet<>(fields));
        }
        result.copyFieldsFrom(this);
        return result;
    }

    @Override
    public void copyFieldsFrom(BasicCursor c) {
        TestTableVCursor from = (TestTableVCursor)c;
        this.id = from.id;
    }

    @Override
    public Iterator<TestTableVCursor> iterator() {
        return new CursorIterator<TestTableVCursor>(this);
    }

    @Override
    protected String _grainName() {
        return "test";
    }

    @Override
    protected String _objectName() {
        return "testTableV";
    }
}

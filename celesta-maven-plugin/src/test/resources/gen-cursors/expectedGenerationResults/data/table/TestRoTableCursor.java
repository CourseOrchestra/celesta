package data.table;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Generated;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.ICelesta;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.CursorIterator;
import ru.curs.celesta.dbutils.ReadOnlyTableCursor;
import ru.curs.celesta.score.ColumnMeta;
import ru.curs.celesta.score.ReadOnlyTable;

@Generated(
        value = "ru.curs.celesta.plugin.maven.CursorGenerator",
        date = "2020-02-25 10:50"
)
public final class TestRoTableCursor extends ReadOnlyTableCursor implements Iterable<TestRoTableCursor> {

    private static final String GRAIN_NAME = "test";
    private static final String OBJECT_NAME = "testRoTable";

    public final TestRoTableCursor.Columns COLUMNS;

    private Integer id;

    {
        this.COLUMNS = new TestRoTableCursor.Columns(callContext().getCelesta());
    }

    public TestRoTableCursor(CallContext context) {
        super(context);
    }

    public TestRoTableCursor(CallContext context, ColumnMeta<?>... columns) {
        super(context, columns);
    }

    @Deprecated
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
    protected void _parseResult(ResultSet rs) throws SQLException {
        if (this.inRec("id")) {
            this.id = rs.getInt("id");
            if (rs.wasNull()) {
                this.id = null;
            }
        }
    }

    @Override
    public void _clearBuffer(boolean withKeys) {
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
            date = "2020-02-25T10:50:49"
    )
    public static final class Columns {
        private final ReadOnlyTable element;

        public Columns(ICelesta celesta) {
            this.element = celesta.getScore().getGrains().get(GRAIN_NAME).getElements(ReadOnlyTable.class).get(OBJECT_NAME);
        }

        public ColumnMeta<Integer> id() {
            return (ColumnMeta<Integer>) this.element.getColumns().get("id");
        }
    }

    @Generated(
            value = "ru.curs.celesta.plugin.maven.CursorGenerator",
            date = "2020-02-25T10:50:49"
    )
    public static final class Id {
        public static final Integer open = 0;
        public static final Integer closed = 1;

        private Id() {
            throw new AssertionError();
        }
    }

}

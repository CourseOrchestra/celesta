package data.view;

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
import ru.curs.celesta.dbutils.CelestaGenerated;
import ru.curs.celesta.dbutils.CursorIterator;
import ru.curs.celesta.dbutils.ViewCursor;
import ru.curs.celesta.score.ColumnMeta;
import ru.curs.celesta.score.View;

@Generated(
        value = "ru.curs.celesta.plugin.maven.CursorGenerator",
        date = "2020-02-25T10:50:49"
)
@CelestaGenerated
public final class TestTableVCursor extends ViewCursor implements Iterable<TestTableVCursor> {

    private static final String GRAIN_NAME = "test";
    private static final String OBJECT_NAME = "testTableV";

    public final TestTableVCursor.Columns COLUMNS;

    private Integer id;

    {
        this.COLUMNS = new TestTableVCursor.Columns(callContext().getCelesta());
    }

    public TestTableVCursor(CallContext context) {
        super(context);
    }

    public TestTableVCursor(CallContext context, ColumnMeta<?>... columns) {
        super(context, columns);
    }

    @Deprecated
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
        private final View element;

        public Columns(ICelesta celesta) {
            this.element = celesta.getScore().getGrains().get(GRAIN_NAME).getElements(View.class).get(OBJECT_NAME);
        }

        public ColumnMeta<Integer> id() {
            return (ColumnMeta<Integer>) this.element.getColumns().get("id");
        }
    }

}

package data.view;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
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
        date = "2021-04-15T02:06:38.885"
)
@CelestaGenerated
public class TestTableVCursor extends ViewCursor implements Iterable<TestTableVCursor> {
    private static final String GRAIN_NAME = "test";

    private static final String OBJECT_NAME = "testTableV";

    public final TestTableVCursor.Columns COLUMNS;

    private Integer id;

    private ZonedDateTime toDelete;

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

    public TestTableVCursor setId(Integer id) {
        this.id = id;
        return this;
    }

    public ZonedDateTime getToDelete() {
        return this.toDelete;
    }
    public TestTableVCursor setToDelete(ZonedDateTime toDelete) {
        this.toDelete = toDelete;
        return this;
    }

    @Override
    protected Object _getFieldValue(String name) {
        switch (name) {
            case "id":
                return this.id;
            case "toDelete":
                return this.toDelete;
            default:
                return null;
        }
    }

    @Override
    protected void _setFieldValue(String name, Object value) {
        switch (name) {
            case "id": {
                this.id = (Integer) value;
                break;
            }
            case "toDelete": {
                this.toDelete = (ZonedDateTime) value;
                break;
            }
            default:;
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

        if (this.inRec("toDelete")) {
            Timestamp ts = rs.getTimestamp("toDelete", Calendar.getInstance(TimeZone.getTimeZone("UTC")));
            if (ts != null) {
                this.toDelete = ZonedDateTime.of(ts.toLocalDateTime(), ZoneOffset.systemDefault());
            } else {
                this.toDelete = null;
            }
        }
    }

    @Override
    public void _clearBuffer(boolean withKeys) {
        this.id = null;
        this.toDelete = null;
    }

    @Override
    public Object[] _currentValues() {
        return new Object[] {id, toDelete};
    }

    @Override
    public TestTableVCursor _getBufferCopy(CallContext context, List<String> fields) {
        final TestTableVCursor result;
        if (Objects.isNull(fields)) {
            result = new TestTableVCursor(context);
        }
        else {
            result = new TestTableVCursor(context, new LinkedHashSet<>(fields));
        }
        result.copyFieldsFrom(this);
        return result;
    }

    @Override
    public void copyFieldsFrom(BasicCursor c) {
        TestTableVCursor from = (TestTableVCursor)c;
        this.id = from.id;
        this.toDelete = from.toDelete;
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
            date = "2021-04-15T02:06:38.886"
    )
    @CelestaGenerated
    public static final class Columns {
        private final View element;

        public Columns(ICelesta celesta) {
            this.element = celesta.getScore().getGrains().get(GRAIN_NAME).getElements(View.class).get(OBJECT_NAME);
        }

        public ColumnMeta<Integer> id() {
            return (ColumnMeta<Integer>) this.element.getColumns().get("id");
        }

        public ColumnMeta<ZonedDateTime> toDelete() {
            return (ColumnMeta<ZonedDateTime>) this.element.getColumns().get("toDelete");
        }
    }
}

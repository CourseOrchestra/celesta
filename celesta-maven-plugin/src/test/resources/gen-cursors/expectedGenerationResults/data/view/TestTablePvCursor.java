package data.view;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Generated;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.ICelesta;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.CelestaGenerated;
import ru.curs.celesta.dbutils.CursorIterator;
import ru.curs.celesta.dbutils.ParameterizedViewCursor;
import ru.curs.celesta.score.ColumnMeta;
import ru.curs.celesta.score.ParameterizedView;

@Generated(
        value = "ru.curs.celesta.plugin.maven.CursorGenerator",
        date = "2024-02-11T20:50:48.6796239"
)
@CelestaGenerated
public class TestTablePvCursor extends ParameterizedViewCursor implements Iterable<TestTablePvCursor> {
    private static final String GRAIN_NAME = "test";

    private static final String OBJECT_NAME = "testTablePv";

    public final TestTablePvCursor.Columns COLUMNS;

    private Integer s;

    {
        this.COLUMNS = new TestTablePvCursor.Columns(callContext().getCelesta());
    }

    public TestTablePvCursor(CallContext context, Map<String, Object> parameters) {
        super(context, parameters);
    }

    public TestTablePvCursor(CallContext context, Map<String, Object> parameters,
            ColumnMeta<?>... columns) {
        super(context, parameters, columns);
    }

    public TestTablePvCursor(CallContext context, Integer p) {
        super (context, paramsMap(p));
    }

    public TestTablePvCursor(CallContext context, Integer p, ColumnMeta<?>... columns) {
        super (context, paramsMap(p), columns);
    }

    private static Map<String, Object> paramsMap(Integer p) {
        Map<String,Object> params = new HashMap<>();
        params.put("p", p);
        return params;
    }

    public Integer getS() {
        return this.s;
    }

    public TestTablePvCursor setS(Integer s) {
        this.s = s;
        return this;
    }

    @Override
    protected Object _getFieldValue(String name) {
        switch (name) {
            case "s": return this.s;
            default: return null;
        }
    }

    @Override
    protected void _setFieldValue(String name, Object value) {
        switch (name) {
            case "s": {
                this.s = (Integer) value;
                break;
            }
            default:;
        }
    }

    @Override
    protected void _parseResult(ResultSet rs) throws SQLException {
        if (this.inRec("s")) {
            this.s = rs.getInt("s");
            if (rs.wasNull()) {
                this.s = null;
            }
        }
    }

    @Override
    public void _clearBuffer(boolean withKeys) {
        this.s = null;
    }

    @Override
    public Object[] _currentValues() {
        return new Object[] {s};
    }

    @Override
    public TestTablePvCursor _getBufferCopy(CallContext context,
            Collection<? extends ColumnMeta<?>> fields) {
        final TestTablePvCursor result;
        if (Objects.isNull(fields)) {
            result = new TestTablePvCursor(context, this.parameters);
        }
        else {
            result = new TestTablePvCursor(context, this.parameters, fields.toArray(new ColumnMeta<?>[0]));
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
            date = "2024-02-11T20:50:48.6836261"
    )
    @CelestaGenerated
    public static final class Columns {
        private final ParameterizedView element;

        public Columns(ICelesta celesta) {
            this.element = celesta.getScore().getGrains().get(GRAIN_NAME).getElements(ParameterizedView.class).get(OBJECT_NAME);
        }

        public ColumnMeta<Integer> s() {
            return (ColumnMeta<Integer>) this.element.getColumns().get("s");
        }
    }
}

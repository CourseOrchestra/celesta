package data.view;

import java.lang.reflect.Field;
import java.math.BigDecimal;
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
import ru.curs.celesta.dbutils.MaterializedViewCursor;
import ru.curs.celesta.score.ColumnMeta;
import ru.curs.celesta.score.MaterializedView;

@Generated(
        value = "ru.curs.celesta.plugin.maven.CursorGenerator",
        date = "2021-04-15T02:06:38.892"
)
@CelestaGenerated
public final class TestTableMvCursor extends MaterializedViewCursor implements Iterable<TestTableMvCursor> {
    private static final String GRAIN_NAME = "test";

    private static final String OBJECT_NAME = "testTableMv";

    public final TestTableMvCursor.Columns COLUMNS;

    private Integer surrogate_count;

    private Integer c;

    private BigDecimal cost;

    {
        this.COLUMNS = new TestTableMvCursor.Columns(callContext().getCelesta());
    }

    public TestTableMvCursor(CallContext context) {
        super(context);
    }

    public TestTableMvCursor(CallContext context, ColumnMeta<?>... columns) {
        super(context, columns);
    }

    @Deprecated
    public TestTableMvCursor(CallContext context, Set<String> fields) {
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

    public BigDecimal getCost() {
        return this.cost;
    }

    public void setCost(BigDecimal cost) {
        this.cost = cost;
    }

    @Override
    protected Object _getFieldValue(String name) {
        try {
            Field f = getClass().getDeclaredField(name);
            f.setAccessible(true);
            return f.get(this);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void _setFieldValue(String name, Object value) {
        try {
            Field f = getClass().getDeclaredField(name);
            f.setAccessible(true);
            f.set(this, value);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Object[] _currentKeyValues() {
        return new Object[] {cost};
    }

    @Override
    protected void _parseResult(ResultSet rs) throws SQLException {
        if (this.inRec("surrogate_count")) {
            this.surrogate_count = rs.getInt("surrogate_count");
            if (rs.wasNull()) {
                this.surrogate_count = null;
            }
        }
        if (this.inRec("c")) {
            this.c = rs.getInt("c");
            if (rs.wasNull()) {
                this.c = null;
            }
        }
        if (this.inRec("cost")) {
            this.cost = rs.getBigDecimal("cost");
            if (rs.wasNull()) {
                this.cost = null;
            }
        }
    }

    @Override
    public void _clearBuffer(boolean withKeys) {
        if (withKeys) {
            this.cost = null;
        }
        this.surrogate_count = null;
        this.c = null;
    }

    @Override
    public Object[] _currentValues() {
        return new Object[] {surrogate_count, c, cost};
    }

    @Override
    public TestTableMvCursor _getBufferCopy(CallContext context, List<String> fields) {
        final TestTableMvCursor result;
        if (Objects.isNull(fields)) {
            result = new TestTableMvCursor(context);
        }
        else {
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
        this.cost = from.cost;
    }

    @Override
    public Iterator<TestTableMvCursor> iterator() {
        return new CursorIterator<TestTableMvCursor>(this);
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
            date = "2021-04-15T02:06:38.893"
    )
    @CelestaGenerated
    public static final class Columns {
        private final MaterializedView element;

        public Columns(ICelesta celesta) {
            this.element = celesta.getScore().getGrains().get(GRAIN_NAME).getElements(MaterializedView.class).get(OBJECT_NAME);
        }

        public ColumnMeta<Integer> surrogate_count() {
            return (ColumnMeta<Integer>) this.element.getColumns().get("surrogate_count");
        }

        public ColumnMeta<Integer> c() {
            return (ColumnMeta<Integer>) this.element.getColumns().get("c");
        }

        public ColumnMeta<BigDecimal> cost() {
            return (ColumnMeta<BigDecimal>) this.element.getColumns().get("cost");
        }
    }
}

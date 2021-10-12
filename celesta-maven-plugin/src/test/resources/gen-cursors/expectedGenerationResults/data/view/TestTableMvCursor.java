package data.view;

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
public class TestTableMvCursor extends MaterializedViewCursor implements Iterable<TestTableMvCursor> {
    private static final String GRAIN_NAME = "test";

    private static final String OBJECT_NAME = "testTableMv";

    public final TestTableMvCursor.Columns COLUMNS;

    private Integer surrogateCount;

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

    public Integer getSurrogateCount() {
        return this.surrogateCount;
    }

    public TestTableMvCursor setSurrogateCount(Integer surrogateCount) {
        this.surrogateCount = surrogateCount;
        return this;
    }

    public Integer getC() {
        return this.c;
    }

    public TestTableMvCursor setC(Integer c) {
        this.c = c;
        return this;
    }

    public BigDecimal getCost() {
        return this.cost;
    }

    public TestTableMvCursor setCost(BigDecimal cost) {
        this.cost = cost;
        return this;
    }

    @Override
    protected Object _getFieldValue(String name) {
        switch (name) {
            case "surrogate_count":
                return this.surrogateCount;
            case "c":
                return this.c;
            case "cost":
                return this.cost;
            default:
                return null;
        }
    }

    @Override
    protected void _setFieldValue(String name, Object value) {
        switch (name) {
            case "surrogate_count": {
                this.surrogateCount = (Integer) value;
                break;
            }
            case "c": {
                this.c = (Integer) value;
                break;
            }
            case "cost": {
                this.cost = (BigDecimal) value;
                break;
            }
            default:;
        }
    }

    @Override
    protected Object[] _currentKeyValues() {
        return new Object[] {cost};
    }

    public boolean tryGet(BigDecimal cost) {
        return tryGetByValuesArray(cost);
    }

    public void get(BigDecimal cost) {
        getByValuesArray(cost);
    }

    @Override
    protected void _parseResult(ResultSet rs) throws SQLException {
        if (this.inRec("surrogate_count")) {
            this.surrogateCount = rs.getInt("surrogate_count");
            if (rs.wasNull()) {
                this.surrogateCount = null;
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
        this.surrogateCount = null;
        this.c = null;
    }

    @Override
    public Object[] _currentValues() {
        return new Object[] {surrogateCount, c, cost};
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
        this.surrogateCount = from.surrogateCount;
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

        public ColumnMeta<Integer> surrogateCount() {
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

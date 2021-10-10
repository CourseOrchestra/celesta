package data.table;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Consumer;
import javax.annotation.Generated;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.ICelesta;
import ru.curs.celesta.dbutils.BLOB;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.CelestaGenerated;
import ru.curs.celesta.dbutils.Cursor;
import ru.curs.celesta.dbutils.CursorIterator;
import ru.curs.celesta.event.TriggerType;
import ru.curs.celesta.score.ColumnMeta;
import ru.curs.celesta.score.Table;

@Generated(
        value = "ru.curs.celesta.plugin.maven.CursorGenerator",
        date = "2021-10-10T22:57:53.806"
)
@CelestaGenerated
public class TestSnakeTableCursor extends Cursor implements Iterable<TestSnakeTableCursor> {
    private static final String GRAIN_NAME = "test";

    private static final String OBJECT_NAME = "test_snake_table";

    public final TestSnakeTableCursor.Columns COLUMNS;

    private Integer snakeField;

    private BLOB snakeBlob;

    private Date dateOne;

    private ZonedDateTime dateTwo;

    private String textField;

    {
        this.COLUMNS = new TestSnakeTableCursor.Columns(callContext().getCelesta());
    }

    public TestSnakeTableCursor(CallContext context) {
        super(context);
    }

    public TestSnakeTableCursor(CallContext context, ColumnMeta<?>... columns) {
        super(context, columns);
    }

    @Deprecated
    public TestSnakeTableCursor(CallContext context, Set<String> fields) {
        super(context, fields);
    }

    public Integer getSnakeField() {
        return this.snakeField;
    }

    public TestSnakeTableCursor setSnakeField(Integer snakeField) {
        this.snakeField = snakeField;
        return this;
    }

    public BLOB getSnakeBlob() {
        return this.snakeBlob;
    }

    public TestSnakeTableCursor setSnakeBlob(BLOB snakeBlob) {
        this.snakeBlob = snakeBlob;
        return this;
    }

    public Date getDateOne() {
        return this.dateOne;
    }

    public TestSnakeTableCursor setDateOne(Date dateOne) {
        this.dateOne = dateOne;
        return this;
    }

    public ZonedDateTime getDateTwo() {
        return this.dateTwo;
    }

    public TestSnakeTableCursor setDateTwo(ZonedDateTime dateTwo) {
        this.dateTwo = dateTwo;
        return this;
    }

    public String getTextField() {
        return this.textField;
    }

    public TestSnakeTableCursor setTextField(String textField) {
        this.textField = textField;
        return this;
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
        return new Object[] {snakeField};
    }

    public boolean tryGet(Integer snakeField) {
        return tryGetByValuesArray(snakeField);
    }

    public void get(Integer snakeField) {
        getByValuesArray(snakeField);
    }

    @Override
    protected void _parseResultInternal(ResultSet rs) throws SQLException {
        if (this.inRec("snake_field")) {
            this.snakeField = rs.getInt("snake_field");
            if (rs.wasNull()) {
                this.snakeField = null;
            }
        }
        this.snakeBlob = null;
        if (this.inRec("date_one")) {
            this.dateOne = rs.getTimestamp("date_one");
            if (rs.wasNull()) {
                this.dateOne = null;
            }
        }
        if (this.inRec("date_two")) {
            Timestamp ts = rs.getTimestamp("date_two", Calendar.getInstance(TimeZone.getTimeZone("UTC")));
            if (ts != null) {
                this.dateTwo = ZonedDateTime.of(ts.toLocalDateTime(), ZoneOffset.systemDefault());
            }
            else {
                this.dateTwo = null;
            }
        }
        if (this.inRec("text_field")) {
            this.textField = rs.getString("text_field");
            if (rs.wasNull()) {
                this.textField = null;
            }
        }
        this.setRecversion(rs.getInt("recversion"));
    }

    @Override
    public void _clearBuffer(boolean withKeys) {
        if (withKeys) {
            this.snakeField = null;
        }
        this.snakeBlob = null;
        this.dateOne = null;
        this.dateTwo = null;
        this.textField = null;
    }

    @Override
    public Object[] _currentValues() {
        return new Object[] {snakeField, snakeBlob, dateOne, dateTwo, textField};
    }

    public void calcSnakeBlob() {
        this.snakeBlob = this.calcBlob("snake_blob");
        ((TestSnakeTableCursor)this.getXRec()).snakeBlob = this.snakeBlob.clone();
    }

    @Override
    protected void _setAutoIncrement(int val) {
    }

    public static void onPreDelete(ICelesta celesta,
            Consumer<? super TestSnakeTableCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.PRE_DELETE, TestSnakeTableCursor.class, cursorConsumer);
    }

    public static void onPostDelete(ICelesta celesta,
            Consumer<? super TestSnakeTableCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.POST_DELETE, TestSnakeTableCursor.class, cursorConsumer);
    }

    public static void onPreInsert(ICelesta celesta,
            Consumer<? super TestSnakeTableCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.PRE_INSERT, TestSnakeTableCursor.class, cursorConsumer);
    }

    public static void onPostInsert(ICelesta celesta,
            Consumer<? super TestSnakeTableCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.POST_INSERT, TestSnakeTableCursor.class, cursorConsumer);
    }

    public static void onPreUpdate(ICelesta celesta,
            Consumer<? super TestSnakeTableCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.PRE_UPDATE, TestSnakeTableCursor.class, cursorConsumer);
    }

    public static void onPostUpdate(ICelesta celesta,
            Consumer<? super TestSnakeTableCursor> cursorConsumer) {
        celesta.getTriggerDispatcher().registerTrigger(TriggerType.POST_UPDATE, TestSnakeTableCursor.class, cursorConsumer);
    }

    @Override
    public TestSnakeTableCursor _getBufferCopy(CallContext context, List<String> fields) {
        final TestSnakeTableCursor result;
        if (Objects.isNull(fields)) {
            result = new TestSnakeTableCursor(context);
        }
        else {
            result = new TestSnakeTableCursor(context, new LinkedHashSet<>(fields));
        }
        result.copyFieldsFrom(this);
        return result;
    }

    @Override
    public void copyFieldsFrom(BasicCursor c) {
        TestSnakeTableCursor from = (TestSnakeTableCursor)c;
        this.snakeField = from.snakeField;
        this.snakeBlob = from.snakeBlob;
        this.dateOne = from.dateOne;
        this.dateTwo = from.dateTwo;
        this.textField = from.textField;
        this.setRecversion(from.getRecversion());
    }

    @Override
    public Iterator<TestSnakeTableCursor> iterator() {
        return new CursorIterator<TestSnakeTableCursor>(this);
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
            date = "2021-10-10T22:57:53.807"
    )
    @CelestaGenerated
    public static final class Columns {
        private final Table element;

        public Columns(ICelesta celesta) {
            this.element = celesta.getScore().getGrains().get(GRAIN_NAME).getElements(Table.class).get(OBJECT_NAME);
        }

        public ColumnMeta<Integer> snakeField() {
            return (ColumnMeta<Integer>) this.element.getColumns().get("snake_field");
        }

        public ColumnMeta<Date> dateOne() {
            return (ColumnMeta<Date>) this.element.getColumns().get("date_one");
        }

        public ColumnMeta<ZonedDateTime> dateTwo() {
            return (ColumnMeta<ZonedDateTime>) this.element.getColumns().get("date_two");
        }

        public ColumnMeta<String> textField() {
            return (ColumnMeta<String>) this.element.getColumns().get("text_field");
        }
    }
} 
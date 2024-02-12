package data.table;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Objects;
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
        date = "2024-02-11T20:38:45.7602869"
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

    private Integer statusField;

    {
        this.COLUMNS = new TestSnakeTableCursor.Columns(callContext().getCelesta());
    }

    public TestSnakeTableCursor(CallContext context) {
        super(context);
    }

    public TestSnakeTableCursor(CallContext context, ColumnMeta<?>... columns) {
        super(context, columns);
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

    public Integer getStatusField() {
        return this.statusField;
    }

    public TestSnakeTableCursor setStatusField(Integer statusField) {
        this.statusField = statusField;
        return this;
    }

    @Override
    protected Object _getFieldValue(String name) {
        switch (name) {
            case "snake_field": return this.snakeField;
            case "snake_blob": return this.snakeBlob;
            case "date_one": return this.dateOne;
            case "date_two": return this.dateTwo;
            case "text_field": return this.textField;
            case "status_field": return this.statusField;
            default: return null;
        }
    }

    @Override
    protected void _setFieldValue(String name, Object value) {
        switch (name) {
            case "snake_field": {
                this.snakeField = (Integer) value;
                break;
            }
            case "snake_blob": {
                this.snakeBlob = (BLOB) value;
                break;
            }
            case "date_one": {
                this.dateOne = (Date) value;
                break;
            }
            case "date_two": {
                this.dateTwo = (ZonedDateTime) value;
                break;
            }
            case "text_field": {
                this.textField = (String) value;
                break;
            }
            case "status_field": {
                this.statusField = (Integer) value;
                break;
            }
            default:;
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
        if (this.inRec("status_field")) {
            this.statusField = rs.getInt("status_field");
            if (rs.wasNull()) {
                this.statusField = null;
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
        this.statusField = null;
    }

    @Override
    public Object[] _currentValues() {
        return new Object[] {snakeField, snakeBlob, dateOne, dateTwo, textField, statusField};
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
    public TestSnakeTableCursor _getBufferCopy(CallContext context,
                                               Collection<? extends ColumnMeta<?>> fields) {
        final TestSnakeTableCursor result;
        if (Objects.isNull(fields)) {
            result = new TestSnakeTableCursor(context);
        }
        else {
            result = new TestSnakeTableCursor(context, fields.toArray(new ColumnMeta<?>[0]));
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
        this.statusField = from.statusField;
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
            date = "2024-02-11T20:38:45.7627849"
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

        public ColumnMeta<Integer> statusField() {
            return (ColumnMeta<Integer>) this.element.getColumns().get("status_field");
        }
    }

    @Generated(
            value = "ru.curs.celesta.plugin.maven.CursorGenerator",
            date = "2024-02-11T20:38:45.7637843"
    )
    @CelestaGenerated
    public static final class StatusField {
        public static final Integer open = 0;

        public static final Integer closed = 1;

        private StatusField() {
            throw new AssertionError();
        }
    }
}

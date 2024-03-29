package ru.curs.celesta.script;

import fieldslimitation.ACursor;
import fieldslimitation.AmvCursor;
import fieldslimitation.AvCursor;
import org.junit.jupiter.api.TestTemplate;
import ru.curs.celesta.CallContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


public class TestFieldsLimitation implements ScriptTest {

    @TestTemplate
    public void test_get_on_table(CallContext context) {
        _clear_table(context);
        int id1 = _insert(context, "A", 5, 1);
        int id2 = _insert(context, "B", 2, 4);

        ACursor.Columns tableColumns = new ACursor.Columns(context.getCelesta());
        ACursor tableCursor = new ACursor(context, tableColumns.numb(), tableColumns.var());

        tableCursor.get(id1);
        assertEquals(id1, tableCursor.getId().intValue());
        assertEquals(5, tableCursor.getNumb().intValue());
        assertEquals("A", tableCursor.getVar());
        assertNull(tableCursor.getAge());

        tableCursor.get(id2);
        assertEquals(id2, tableCursor.getId().intValue());
        assertEquals(2, tableCursor.getNumb().intValue());
        assertEquals("B", tableCursor.getVar());
        assertNull(tableCursor.getAge());
    }

    @TestTemplate
    public void test_get_on_materialized_view(CallContext context) {
        _clear_table(context);
        _insert(context, "A", 5, 1);
        _insert(context, "B", 2, 4);

        AmvCursor.Columns mvColumns = new AmvCursor.Columns(context.getCelesta());
        AmvCursor mvCursor = new AmvCursor(context, mvColumns.numb());

        mvCursor.get("A");
        assertNull(mvCursor.getId());
        assertEquals(5, mvCursor.getNumb().intValue());
        assertEquals("A", mvCursor.getVar());
        assertNull(mvCursor.getAge());

        mvCursor.get("B");
        assertNull(mvCursor.getId());
        assertEquals(2, mvCursor.getNumb().intValue());
        assertEquals("B", mvCursor.getVar());
        assertNull(mvCursor.getAge());
    }

    @TestTemplate
    public void test_set_on_table(CallContext context) {
        ACursor.Columns tableColumns = new ACursor.Columns(context.getCelesta());
        ACursor tableCursor = new ACursor(context, tableColumns.numb(), tableColumns.var());
        _test_set(context, tableCursor);
    }

    @TestTemplate
    public void test_set_on_view(CallContext context) {
        AvCursor.Columns viewColumns = new AvCursor.Columns(context.getCelesta());
        AvCursor viewCursor = new AvCursor(context, viewColumns.numb(), viewColumns.var());
        _test_set(context, viewCursor);
    }

    @TestTemplate
    public void test_set_on_materialized_view(CallContext context) {
        AmvCursor.Columns mvColumns = new AmvCursor.Columns(context.getCelesta());
        AmvCursor mvCursor = new AmvCursor(context, mvColumns.numb(), mvColumns.var());
        _test_set(context, mvCursor);
    }

    @TestTemplate
    public void test_navigation_on_table(CallContext context) {
        ACursor.Columns tableColumns = new ACursor.Columns(context.getCelesta());
        ACursor tableCursor = new ACursor(context, tableColumns.numb(), tableColumns.var());
        _test_navigation(context, tableCursor);
    }

    @TestTemplate
    public void test_navigation_on_view(CallContext context) {
        AvCursor.Columns viewColumns = new AvCursor.Columns(context.getCelesta());
        AvCursor viewCursor = new AvCursor(context, viewColumns.numb(), viewColumns.var());
        _test_navigation(context, viewCursor);
    }

    @TestTemplate
    public void test_navigation_on_materialized_view(CallContext context) {
        AmvCursor.Columns mvColumns = new AmvCursor.Columns(context.getCelesta());
        AmvCursor mvCursor = new AmvCursor(context, mvColumns.numb(), mvColumns.var());
        _test_navigation(context, mvCursor);
    }

    private void _test_set(CallContext context, ACursor cursor) {
        _clear_table(context);
        _insert(context, "A", 5, 1);
        _insert(context, "B", 2, 4);

        cursor.orderBy(cursor.COLUMNS.numb().desc());
        cursor.findSet();
        assertEquals(5, cursor.getNumb().intValue());
        assertEquals("A", cursor.getVar());
        assertNull(cursor.getAge());

        cursor.nextInSet();
        assertEquals(2, cursor.getNumb().intValue());
        assertEquals("B", cursor.getVar());
        assertNull(cursor.getAge());
    }

    private void _test_set(CallContext context, AvCursor cursor) {
        _clear_table(context);
        _insert(context, "A", 5, 1);
        _insert(context, "B", 2, 4);

        cursor.orderBy(cursor.COLUMNS.numb().desc());
        cursor.findSet();
        assertEquals(5, cursor.getNumb().intValue());
        assertEquals("A", cursor.getVar());
        assertNull(cursor.getAge());

        cursor.nextInSet();
        assertEquals(2, cursor.getNumb().intValue());
        assertEquals("B", cursor.getVar());
        assertNull(cursor.getAge());
    }

    private void _test_set(CallContext context, AmvCursor cursor) {
        _clear_table(context);
        _insert(context, "A", 5, 1);
        _insert(context, "B", 2, 4);

        cursor.orderBy(cursor.COLUMNS.numb().desc());
        cursor.findSet();
        assertEquals(5, cursor.getNumb().intValue());
        assertEquals("A", cursor.getVar());
        assertNull(cursor.getAge());

        cursor.nextInSet();
        assertEquals(2, cursor.getNumb().intValue());
        assertEquals("B", cursor.getVar());
        assertNull(cursor.getAge());
    }

    private void _test_navigation(CallContext context, ACursor cursor) {
        _clear_table(context);
        _insert(context, "A", 5, 1);
        _insert(context, "B", 2, 4);

        cursor.orderBy(cursor.COLUMNS.numb().desc());
        cursor.first();
        assertEquals(5, cursor.getNumb().intValue());
        assertEquals("A", cursor.getVar());
        assertNull(cursor.getAge());

        cursor.next();
        assertEquals(2, cursor.getNumb().intValue());
        assertEquals("B", cursor.getVar());
        assertNull(cursor.getAge());

        cursor.navigate("=");
        assertEquals(2, cursor.getNumb().intValue());
        assertEquals("B", cursor.getVar());
        assertNull(cursor.getAge());

        cursor.previous();
        assertEquals(5, cursor.getNumb().intValue());
        assertEquals("A", cursor.getVar());
        assertNull(cursor.getAge());

        cursor.last();
        assertEquals(2, cursor.getNumb().intValue());
        assertEquals("B", cursor.getVar());
        assertNull(cursor.getAge());
    }

    private void _test_navigation(CallContext context, AvCursor cursor) {
        _clear_table(context);
        _insert(context, "A", 5, 1);
        _insert(context, "B", 2, 4);

        cursor.orderBy(cursor.COLUMNS.numb().desc());
        cursor.first();
        assertEquals(5, cursor.getNumb().intValue());
        assertEquals("A", cursor.getVar());
        assertNull(cursor.getAge());

        cursor.next();
        assertEquals(2, cursor.getNumb().intValue());
        assertEquals("B", cursor.getVar());
        assertNull(cursor.getAge());

        cursor.navigate("=");
        assertEquals(2, cursor.getNumb().intValue());
        assertEquals("B", cursor.getVar());
        assertNull(cursor.getAge());

        cursor.previous();
        assertEquals(5, cursor.getNumb().intValue());
        assertEquals("A", cursor.getVar());
        assertNull(cursor.getAge());

        cursor.last();
        assertEquals(2, cursor.getNumb().intValue());
        assertEquals("B", cursor.getVar());
        assertNull(cursor.getAge());
    }

    private void _test_navigation(CallContext context, AmvCursor cursor) {
        _clear_table(context);
        _insert(context, "A", 5, 1);
        _insert(context, "B", 2, 4);

        cursor.orderBy(cursor.COLUMNS.numb().desc());
        cursor.first();
        assertEquals(5, cursor.getNumb().intValue());
        assertEquals("A", cursor.getVar());
        assertNull(cursor.getAge());

        cursor.next();
        assertEquals(2, cursor.getNumb().intValue());
        assertEquals("B", cursor.getVar());
        assertNull(cursor.getAge());

        cursor.navigate("=");
        assertEquals(2, cursor.getNumb().intValue());
        assertEquals("B", cursor.getVar());
        assertNull(cursor.getAge());

        cursor.previous();
        assertEquals(5, cursor.getNumb().intValue());
        assertEquals("A", cursor.getVar());
        assertNull(cursor.getAge());

        cursor.last();
        assertEquals(2, cursor.getNumb().intValue());
        assertEquals("B", cursor.getVar());
        assertNull(cursor.getAge());
    }

    private void _clear_table(CallContext context) {
        ACursor tableCursor = new ACursor(context);
        tableCursor.deleteAll();
    }

    private int _insert(CallContext context, String var, Integer numb, Integer age) {
        ACursor tableCursor = new ACursor(context);

        tableCursor.setVar(var);
        tableCursor.setNumb(numb);
        tableCursor.setAge(age);
        tableCursor.insert();
        int id = tableCursor.getId();
        tableCursor.clear();

        return id;
    }

}

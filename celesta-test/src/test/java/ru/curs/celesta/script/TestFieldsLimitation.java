package ru.curs.celesta.script;

import fieldslimitation.ACursor;
import fieldslimitation.AmvCursor;
import fieldslimitation.AvCursor;
import org.junit.jupiter.api.TestTemplate;
import ru.curs.celesta.CallContext;

import java.util.Arrays;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;


class TestFieldsLimitation implements ScriptTest {

    @TestTemplate
    void test_get_on_table(CallContext context) {
        _clear_table(context);
        int id1 = _insert(context, "A", 5, 1);
        int id2 = _insert(context, "B", 2, 4);


        ACursor tableCursor = new ACursor(context,
                new HashSet<String>(Arrays.asList(new String[]{"numb", "var"})));

        tableCursor.get(id1);
        assertEquals(id1, tableCursor.getId().intValue());
        assertEquals(5, tableCursor.getNumb().intValue());
        assertEquals("A", tableCursor.getVar());
        assertEquals(null, tableCursor.getAge());

        tableCursor.get(id2);
        assertEquals(id2, tableCursor.getId().intValue());
        assertEquals(2, tableCursor.getNumb().intValue());
        assertEquals("B", tableCursor.getVar());
        assertEquals(null, tableCursor.getAge());

    }

    @TestTemplate
    void test_get_on_materialized_view(CallContext context) {
        _clear_table(context);
        _insert(context, "A", 5, 1);
        _insert(context, "B", 2, 4);

        AmvCursor mvCursor = new AmvCursor(context, new HashSet<String>(Arrays.asList(new String[]{"numb"})));

        mvCursor.get("A");
        assertEquals(null, mvCursor.getId());
        assertEquals(5, mvCursor.getNumb().intValue());
        assertEquals("A", mvCursor.getVar());
        assertEquals(null, mvCursor.getAge());

        mvCursor.get("B");
        assertEquals(null, mvCursor.getId());
        assertEquals(2, mvCursor.getNumb().intValue());
        assertEquals("B", mvCursor.getVar());
        assertEquals(null, mvCursor.getAge());

    }

    @TestTemplate
    void test_set_on_table(CallContext context) {
        ACursor tableCursor = new ACursor(context, new HashSet<String>(Arrays.asList(new String[]{"numb", "var"})));
        _test_set(context, tableCursor);

    }

    @TestTemplate
    void test_set_on_view(CallContext context) {
        AvCursor viewCursor = new AvCursor(context, new HashSet<String>(Arrays.asList(new String[]{"numb", "var"})));
        _test_set(context, viewCursor);

    }

    @TestTemplate
    void test_set_on_materialized_view(CallContext context) {
        AvCursor viewCursor = new AvCursor(context, new HashSet<String>(Arrays.asList(new String[]{"numb", "var"})));
        _test_set(context, viewCursor);

    }

    @TestTemplate
    void test_navigation_on_table(CallContext context) {
        ACursor tableCursor = new ACursor(context, new HashSet<String>(Arrays.asList(new String[]{"numb", "var"})));
        _test_navigation(context, tableCursor);

    }

    @TestTemplate
    void test_navigation_on_view(CallContext context) {
        AvCursor viewCursor = new AvCursor(context, new HashSet<String>(Arrays.asList(new String[]{"numb", "var"})));
        _test_navigation(context, viewCursor);

    }

    @TestTemplate
    void test_navigation_on_materialized_view(CallContext context) {
        AvCursor viewCursor = new AvCursor(context, new HashSet<String>(Arrays.asList(new String[]{"numb", "var"})));
        _test_navigation(context, viewCursor);

    }

    void _test_set(CallContext context, ACursor cursor) {
        _clear_table(context);
        _insert(context, "A", 5, 1);
        _insert(context, "B", 2, 4);

        cursor.orderBy(ACursor.numb_COLUMN.desc());
        cursor.findSet();
        assertEquals(5, cursor.getNumb().intValue());
        assertEquals("A", cursor.getVar());
        assertEquals(null, cursor.getAge());

        cursor.nextInSet();
        assertEquals(2, cursor.getNumb().intValue());
        assertEquals("B", cursor.getVar());
        assertEquals(null, cursor.getAge());

    }

    void _test_set(CallContext context, AvCursor cursor) {
        _clear_table(context);
        _insert(context, "A", 5, 1);
        _insert(context, "B", 2, 4);

        cursor.orderBy(ACursor.numb_COLUMN.desc());
        cursor.findSet();
        assertEquals(5, cursor.getNumb().intValue());
        assertEquals("A", cursor.getVar());
        assertEquals(null, cursor.getAge());

        cursor.nextInSet();
        assertEquals(2, cursor.getNumb().intValue());
        assertEquals("B", cursor.getVar());
        assertEquals(null, cursor.getAge());

    }

    void _test_navigation(CallContext context, ACursor cursor) {
        _clear_table(context);
        _insert(context, "A", 5, 1);
        _insert(context, "B", 2, 4);

        cursor.orderBy(ACursor.numb_COLUMN.desc());
        cursor.first();
        assertEquals(5, cursor.getNumb().intValue());
        assertEquals("A", cursor.getVar());
        assertEquals(null, cursor.getAge());

        cursor.next();
        assertEquals(2, cursor.getNumb().intValue());
        assertEquals("B", cursor.getVar());
        assertEquals(null, cursor.getAge());

        cursor.navigate("=");
        assertEquals(2, cursor.getNumb().intValue());
        assertEquals("B", cursor.getVar());
        assertEquals(null, cursor.getAge());

        cursor.previous();
        assertEquals(5, cursor.getNumb().intValue());
        assertEquals("A", cursor.getVar());
        assertEquals(null, cursor.getAge());

        cursor.last();
        assertEquals(2, cursor.getNumb().intValue());
        assertEquals("B", cursor.getVar());
        assertEquals(null, cursor.getAge());

    }

    void _test_navigation(CallContext context, AvCursor cursor) {
        _clear_table(context);
        _insert(context, "A", 5, 1);
        _insert(context, "B", 2, 4);

        cursor.orderBy(AvCursor.numb_COLUMN.desc());
        cursor.first();
        assertEquals(5, cursor.getNumb().intValue());
        assertEquals("A", cursor.getVar());
        assertEquals(null, cursor.getAge());

        cursor.next();
        assertEquals(2, cursor.getNumb().intValue());
        assertEquals("B", cursor.getVar());
        assertEquals(null, cursor.getAge());

        cursor.navigate("=");
        assertEquals(2, cursor.getNumb().intValue());
        assertEquals("B", cursor.getVar());
        assertEquals(null, cursor.getAge());

        cursor.previous();
        assertEquals(5, cursor.getNumb().intValue());
        assertEquals("A", cursor.getVar());
        assertEquals(null, cursor.getAge());

        cursor.last();
        assertEquals(2, cursor.getNumb().intValue());
        assertEquals("B", cursor.getVar());
        assertEquals(null, cursor.getAge());
    }

    void _clear_table(CallContext context) {
        ACursor tableCursor = new ACursor(context);
        tableCursor.deleteAll();
    }


    int _insert(CallContext context, String var, Integer numb, Integer age) {
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

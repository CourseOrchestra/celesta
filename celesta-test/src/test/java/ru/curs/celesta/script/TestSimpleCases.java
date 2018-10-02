package ru.curs.celesta.script;

import org.junit.jupiter.api.TestTemplate;
import org.apache.commons.lang.mutable.MutableBoolean;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.syscursors.LogCursor;
import simpleCases.*;

import java.sql.Timestamp;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

class TestSimpleCases implements ScriptTest {

    @TestTemplate
    void test_getdate_in_view(CallContext context) {
        GetDateForViewCursor tableCursor = new GetDateForViewCursor(context);
        tableCursor.deleteAll();

        ViewWithGetDateCursor viewCursor = new ViewWithGetDateCursor(context);
        assertEquals(0, viewCursor.count());

        tableCursor.setDate(Timestamp.valueOf(LocalDateTime.now().minusDays(1)));
        tableCursor.insert();
        assertEquals(0, viewCursor.count());

        tableCursor.clear();
        tableCursor.setDate(Timestamp.valueOf(LocalDateTime.now().plusDays(1)));
        tableCursor.insert();
        assertEquals(1, viewCursor.count());


    }

    @TestTemplate
    void test_zero_insert(CallContext context) {
        ZeroInsertCursor c = new ZeroInsertCursor(context);
        c.deleteAll();

        c.insert();
        System.out.println(c.getId());
        System.out.println(c.getDate());


    }

    @TestTemplate
    void test_triggers_on_sys_cursors(CallContext context) {
        LogCursor c = new LogCursor(context);
        MutableBoolean isPreDeleteDone = new MutableBoolean(false);
        MutableBoolean isPostDeleteDone = new MutableBoolean(false);

        LogCursor.onPreInsert(context.getCelesta(), logCursor ->
                logCursor.setTablename("getDateForView"));
        LogCursor.onPostInsert(context.getCelesta(), logCursor ->
                logCursor.setSessionid("1"));
        LogCursor.onPreUpdate(context.getCelesta(), logCursor ->
                logCursor.setTablename("zeroInsert"));
        LogCursor.onPostUpdate(context.getCelesta(), logCursor ->
                logCursor.setSessionid("2"));
        LogCursor.onPreDelete(context.getCelesta(), logCursor -> {
            isPreDeleteDone.setValue(true);
            logCursor.setTablename("table2");
        });
        LogCursor.onPostDelete(context.getCelesta(), logCursor -> {
            isPostDeleteDone.setValue(true);
            logCursor.setSessionid("2");
        });

        c.setUserid("1");
        c.setSessionid("0");
        c.setGrainid("simpleCases");
        c.setTablename("zeroInsert");
        c.setAction_type("I");
        c.insert();

        assertEquals("getDateForView", c.getTablename());
        assertEquals("1", c.getSessionid());

        c.update();

        assertEquals("zeroInsert", c.getTablename());
        assertEquals("2", c.getSessionid());

        assertFalse(isPreDeleteDone.booleanValue());
        assertFalse(isPostDeleteDone.booleanValue());

        c.delete();

        assertTrue(isPreDeleteDone.booleanValue());
        assertTrue(isPostDeleteDone.booleanValue());

    }

    @TestTemplate
    void test_triggers_on_gen_cursors(CallContext context) {
        ForTriggersCursor c = new ForTriggersCursor(context);
        MutableBoolean isPreDeleteDone = new MutableBoolean(false);
        MutableBoolean isPostDeleteDone = new MutableBoolean(false);

        ForTriggersCursor.onPreInsert(context.getCelesta(), forTriggersCursor -> {
            CustomSequence s = new CustomSequence(forTriggersCursor.callContext());
            forTriggersCursor.setId((int) s.nextValue());
        });
        ForTriggersCursor.onPostInsert(context.getCelesta(), forTriggersCursor -> {
            forTriggersCursor.setVal(2);
        });
        ForTriggersCursor.onPreUpdate(context.getCelesta(), forTriggersCursor -> {
            forTriggersCursor.setVal(3);
        });
        ForTriggersCursor.onPostUpdate(context.getCelesta(), forTriggersCursor -> {
            CustomSequence s = new CustomSequence(forTriggersCursor.callContext());
            forTriggersCursor.setId((int) s.nextValue());
        });
        ForTriggersCursor.onPreDelete(context.getCelesta(), forTriggersCursor -> {
            isPreDeleteDone.setValue(true);
        });
        ForTriggersCursor.onPostDelete(context.getCelesta(), forTriggersCursor -> {
            isPostDeleteDone.setValue(true);
        });

        c.insert();

        assertEquals(1, c.getId().intValue());
        assertEquals(2, c.getVal().intValue());

        c.update();

        assertEquals(2L, c.getId().intValue());
        assertEquals(3, c.getVal().intValue());

        assertFalse(isPreDeleteDone.booleanValue());
        assertFalse(isPostDeleteDone.booleanValue());

        c.setId(1);

        c.delete();

        assertTrue(isPreDeleteDone.booleanValue());
        assertTrue(isPostDeleteDone.booleanValue());


    }

    @TestTemplate
    void test_try_insert(CallContext context) {
        DuplicateCursor c = new DuplicateCursor(context);
        c.deleteAll();
        c.setId(10);
        assertTrue(c.tryInsert());
        assertFalse(c.tryInsert());
        c.setId(12);
        assertTrue(c.tryInsert());

    }
}

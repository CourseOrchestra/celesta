package ru.curs.celesta.script;

import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.syscursors.LogCursor;
import simpleCases.CustomSequence;
import simpleCases.DuplicateCursor;
import simpleCases.ForTriggersCursor;
import simpleCases.GetDateForViewCursor;
import simpleCases.SimpleTableCursor;
import simpleCases.UsesequenceCursor;
import simpleCases.ViewWithGetDateCursor;
import simpleCases.ZeroInsertCursor;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

public class TestSimpleCases implements ScriptTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestSimpleCases.class);

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
        LOGGER.info("{}", c.getId());
        LOGGER.info("{}", c.getDate());
    }

    @TestTemplate
    void testInsertWithSequence(CallContext context) {
        UsesequenceCursor c = new UsesequenceCursor(context);
        c.setId(100);
        c.insert();
        //A value is provided by the sequence
        assertTrue(c.getVal() > 0);
    }

    @TestTemplate
    void test_triggers_on_sys_cursors(CallContext context) {
        LogCursor c = new LogCursor(context);
        AtomicBoolean isPreDeleteDone = new AtomicBoolean(false);
        AtomicBoolean isPostDeleteDone = new AtomicBoolean(false);

        LogCursor.onPreInsert(context.getCelesta(), logCursor ->
                logCursor.setTablename("getDateForView"));
        LogCursor.onPostInsert(context.getCelesta(), logCursor ->
                logCursor.setSessionid("1"));
        LogCursor.onPreUpdate(context.getCelesta(), logCursor ->
                logCursor.setTablename("zeroInsert"));
        LogCursor.onPostUpdate(context.getCelesta(), logCursor ->
                logCursor.setSessionid("2"));
        LogCursor.onPreDelete(context.getCelesta(), logCursor -> {
            isPreDeleteDone.set(true);
            logCursor.setTablename("table2");
        });
        LogCursor.onPostDelete(context.getCelesta(), logCursor -> {
            isPostDeleteDone.set(true);
            logCursor.setSessionid("2");
        });

        c.setUserid("1");
        c.setSessionid("0");
        c.setGrainid("simpleCases");
        c.setTablename("zeroInsert");
        c.setActionType("I");
        c.insert();

        assertEquals("getDateForView", c.getTablename());
        assertEquals("1", c.getSessionid());

        c.update();

        assertEquals("zeroInsert", c.getTablename());
        assertEquals("2", c.getSessionid());

        assertFalse(isPreDeleteDone.get());
        assertFalse(isPostDeleteDone.get());

        c.delete();

        assertTrue(isPreDeleteDone.get());
        assertTrue(isPostDeleteDone.get());
    }

    @TestTemplate
    void test_triggers_on_gen_cursors(CallContext context) {
        ForTriggersCursor c = new ForTriggersCursor(context);
        AtomicBoolean isPreDeleteDone = new AtomicBoolean(false);
        AtomicBoolean isPostDeleteDone = new AtomicBoolean(false);

        ForTriggersCursor.onPreInsert(context.getCelesta(), forTriggersCursor -> {
            CustomSequence s = new CustomSequence(forTriggersCursor.callContext());
            forTriggersCursor.setId((int) s.nextValue());
        });
        ForTriggersCursor.onPostInsert(context.getCelesta(), forTriggersCursor -> forTriggersCursor.setVal(2));
        ForTriggersCursor.onPreUpdate(context.getCelesta(), forTriggersCursor -> forTriggersCursor.setVal(3));
        ForTriggersCursor.onPostUpdate(context.getCelesta(), forTriggersCursor -> {
            CustomSequence s = new CustomSequence(forTriggersCursor.callContext());
            forTriggersCursor.setId((int) s.nextValue());
        });
        ForTriggersCursor.onPreDelete(context.getCelesta(), forTriggersCursor -> isPreDeleteDone.set(true));
        ForTriggersCursor.onPostDelete(context.getCelesta(), forTriggersCursor -> isPostDeleteDone.set(true));

        c.insert();

        assertEquals(1, c.getId().intValue());
        assertEquals(2, c.getVal().intValue());

        c.update();

        assertEquals(2L, c.getId().intValue());
        assertEquals(3, c.getVal().intValue());

        assertFalse(isPreDeleteDone.get());
        assertFalse(isPostDeleteDone.get());

        c.setId(1);

        c.delete();

        assertTrue(isPreDeleteDone.get());
        assertTrue(isPostDeleteDone.get());
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

    @TestTemplate
    void test_cursor_getCurrentKeyValues(CallContext context) {
        SimpleTableCursor c = new SimpleTableCursor(context);
        c.deleteAll();
//        c.setId(1);
        c.setName("ONE");
        c.insert();

        c = new SimpleTableCursor(context);
        c = c.iterator().next();

        Object[] keyValues = c.getCurrentKeyValues();

        assertEquals(1, keyValues.length);
//        assertEquals(1, keyValues[0]);
        assertInstanceOf(Integer.class, keyValues[0]);
    }

    @TestTemplate
    void test_cursor_getCurrentValues(CallContext context) {
        SimpleTableCursor c = new SimpleTableCursor(context);
        c.deleteAll();
//        c.setId(1);
        c.setName("ONE");
        c.insert();

        c = new SimpleTableCursor(context);
        c = c.iterator().next();

        Object[] values = c.getCurrentValues();

        assertEquals(3, values.length);

//        assertEquals(1, values[0]);
        assertInstanceOf(Integer.class, values[0]);

        assertEquals("ONE", values[1]);
        assertInstanceOf(String.class, values[1]);
    }


    @TestTemplate
    void test_autoincremental_insertion(CallContext context) {
        GetDateForViewCursor tableCursor = new GetDateForViewCursor(context);
        tableCursor.deleteAll();
        for (int i = 0; i < 7; i++) {
            tableCursor.clear();
            tableCursor.setDate(new Date());
            tableCursor.insert();
        }
        assertEquals(7, tableCursor.count());
    }

    @TestTemplate
    void test_duplicate_insertion(CallContext context) {
        DuplicateCursor c = new DuplicateCursor(context);
        c.deleteAll();
        c.setId(7);
        c.insert();

        String message = assertThrows(CelestaException.class, c::insert).getMessage();
        assertEquals("Record duplicate [7] already exists", message);
    }

    @TestTemplate
    void test_update_nonexistent(CallContext context) {
        DuplicateCursor c = new DuplicateCursor(context);
        c.deleteAll();
        c.setId(42);
        c.setVal(42);

        String message = assertThrows(CelestaException.class, c::update).getMessage();
        assertEquals("Record duplicate [42] does not exist.", message);
    }
}

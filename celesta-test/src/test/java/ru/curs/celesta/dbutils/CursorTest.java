package ru.curs.celesta.dbutils;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.*;

import ru.curs.celesta.*;
import ru.curs.celesta.syscursors.LogCursor;
import ru.curs.celesta.syscursors.LogsetupCursor;

public class CursorTest extends AbstractCelestaTest {

    @Override
    protected String scorePath() {
        return "score";
    }

    private Cursor c;


    @BeforeEach
    public void before() {
        c = new LogsetupCursor(cc());
    }


    @AfterEach
    public void after() {
        c.close();
    }

    @Test
    public void cursorIsNavigable() {
        LogsetupCursor c2 = (LogsetupCursor) c;
        c.setFilter(c2.COLUMNS.grainid(), "'b'%");
        c2.setGrainid("grainval");
        c2.setTablename("tablenameval");
        c2.setI(true);
        c2.setM(false);
        c2.setD(true);
        assertTrue(
                assertThrows(CelestaException.class,
                        () -> c.navigate("=s><+-")
                ).getMessage().contains("Invalid navigation command")
        );

        c.navigate("=><+-");
    }

    @Test
    public void fieldsAreAssignable() {
        LogsetupCursor lsc = (LogsetupCursor) c;
        assertNull(lsc.getGrainid());
        lsc.setValue("grainid", "asdFsaf");

        assertEquals("asdFsaf", lsc.getGrainid());

        assertTrue(
                assertThrows(CelestaException.class,
                        () -> lsc.setValue("asdfasdf", "sswe")).getMessage()
                        .contains("No column")
        );
    }

    @Test
    public void testClose() throws Exception {
        BasicCursor xRec = c.getXRec();

        Object[] rec = {"f1", "f2", "f3", "f4", "f5"};

        c.getHelper.getHolder().getStatement(rec, 0);
        c.insert.getStatement(rec, 0);

        boolean[] updateMask = {true, false, false, true, true};
        c.updateMask = updateMask;
        boolean[] nullUpdateMask = {false, true, true, false, false};
        c.nullUpdateMask = nullUpdateMask;

        c.update.getStatement(rec, 0);
        c.delete.getStatement(rec, 0);

        c.set.getStatement(rec, 0);
        c.forwards.getStatement(rec, 0);
        c.backwards.getStatement(rec, 0);
        c.here.getStatement(rec, 0);
        c.first.getStatement(rec, 0);
        c.last.getStatement(rec, 0);
        c.count.getStatement(rec, 0);
        c.position.getStatement(rec, 0);


        assertAll(
                () -> assertFalse(c.isClosed()),
                () -> assertFalse(xRec.isClosed()),
                () -> assertTrue(c.getHelper.getHolder().isStmtValid()),
                () -> assertTrue(c.insert.isStmtValid()),
                () -> assertTrue(c.update.isStmtValid()),
                () -> assertTrue(c.delete.isStmtValid()),

                () -> assertTrue(c.set.isStmtValid()),
                () -> assertTrue(c.forwards.isStmtValid()),
                () -> assertTrue(c.backwards.isStmtValid()),
                () -> assertTrue(c.here.isStmtValid()),
                () -> assertTrue(c.first.isStmtValid()),
                () -> assertTrue(c.last.isStmtValid()),
                () -> assertTrue(c.count.isStmtValid()),
                () -> assertTrue(c.position.isStmtValid())
        );

        c.close();

        assertAll(
                () -> assertTrue(xRec.isClosed()),
                () -> assertFalse(c.getHelper.getHolder().isStmtValid()),
                () -> assertFalse(c.insert.isStmtValid()),
                () -> assertFalse(c.update.isStmtValid()),
                () -> assertFalse(c.delete.isStmtValid()),

                () -> assertFalse(c.set.isStmtValid()),
                () -> assertFalse(c.forwards.isStmtValid()),
                () -> assertFalse(c.backwards.isStmtValid()),
                () -> assertFalse(c.here.isStmtValid()),
                () -> assertFalse(c.first.isStmtValid()),
                () -> assertFalse(c.last.isStmtValid()),
                () -> assertFalse(c.count.isStmtValid()),
                () -> assertFalse(c.position.isStmtValid())
        );

    }

    @Test
    void copyFilterFromCopiesFilters() {
        LogsetupCursor c2 = new LogsetupCursor(cc());
        c2.setRange(c2.COLUMNS.m(), true);
        c2.setFilter(c2.COLUMNS.tablename(), "foo%");
        c2.setComplexFilter("i = m");
        c2.limit(5, 10);

        c.setRange(c2.COLUMNS.d(), false);
        c.copyFiltersFrom(c2);

        assertAll(
                () -> assertEquals(c2.getComplexFilter(), c.getComplexFilter()),
                () -> assertNull(c.getFilters().get("d")),
                () -> assertEquals("true", c.getFilters().get("m").toString()),
                () -> assertEquals("foo%", c.getFilters().get("tablename").toString()),
                () -> assertEquals("\"i\" = \"m\"", c.getComplexFilter())
        );
    }

    @Test
    void isEquivalentChecksForFilterEquivalence() {
        LogsetupCursor c2 = new LogsetupCursor(cc());
        c2.setRange(c2.COLUMNS.m(), true);
        c2.setFilter(c2.COLUMNS.tablename(), "foo%");
        //c2 has complexFilter, no complexFilter on c
        c2.setComplexFilter("i = m");

        assertFalse(c.isEquivalent(c2));
        c.copyFiltersFrom(c2);
        assertTrue(c.isEquivalent(c2));

        c.orderBy(c2.COLUMNS.i());
        assertFalse(c.isEquivalent(c2));
        c2.orderBy(c2.COLUMNS.i());
        assertTrue(c.isEquivalent(c2));

        c.reset();
        assertFalse(c.isEquivalent(c2));
        c2.reset();
        assertTrue(c.isEquivalent(c2));

        //c has complexFilter, no complexFilter on c2
        c.setComplexFilter("i > m");
        assertFalse(c.isEquivalent(c2));
    }

    @Test
    void isEquivalentChecksDeepForFilterEquivalence() {
        LogsetupCursor c2 = new LogsetupCursor(cc());
        c2.setRange(c2.COLUMNS.m(), true);
        assertFalse(c.isEquivalent(c2));

        c.setRange(c2.COLUMNS.m(), false);
        assertFalse(c.isEquivalent(c2));

        c.setRange(c2.COLUMNS.m(), true);
        assertTrue(c.isEquivalent(c2));

        c.setComplexFilter("i > m");
        c2.setComplexFilter("i = m");
        assertFalse(c.isEquivalent(c2));
        //extra spaces are intentional: filter should be still equal
        c.setComplexFilter("i   =   m");
        assertTrue(c.isEquivalent(c2));
    }

    @Test
    void positionCalculatesPosition() {
        LogCursor lc = new LogCursor(cc());

        setupLogCursor(lc);
        lc.insert();
        lc.clear();

        setupLogCursor(lc);
        lc.insert();
        lc.clear();

        setupLogCursor(lc);
        lc.insert();
        assertEquals(2, ((BasicCursor) lc).position());

        lc.setFilter(lc.COLUMNS.entryno(), ">1");
        assertEquals(1, ((BasicCursor) lc).position());

    }

    private void setupLogCursor(LogCursor lc) {
        lc.setUserid("foo");
        lc.setGrainid("celesta");
        lc.setTablename("logsetup");
        lc.setAction_type(Action.INSERT.shortId());
    }

    @Test
    void emptyCursorIsNotNavigable() {
        c.deleteAll();
        assertThrows(CelestaException.class, () -> c.first());
        assertThrows(CelestaException.class, () -> c.last());
        assertThrows(CelestaException.class, () -> c.getByValuesArray("foo", "bar"));
    }
}

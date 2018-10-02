package ru.curs.celesta.dbutils;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.*;

import java.sql.SQLException;
import java.util.Properties;


import ru.curs.celesta.*;
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
        c.setFilter("grainid", "'b'%");
        LogsetupCursor c2 = (LogsetupCursor) c;
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
}

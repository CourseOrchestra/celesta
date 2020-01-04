package ru.curs.celesta;

import org.junit.jupiter.api.Test;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.syscursors.*;
import simpleCases.Simple_tableCursor;
import simpleCases.Simple_viewCursor;

import static org.junit.jupiter.api.Assertions.*;

public class InitTest extends AbstractCelestaTest {


    @Test
    public void grainCursorIsCallable() {
        GrainsCursor g = new GrainsCursor(cc());
        assertEquals("grains", g.meta().getName());
        assertEquals(8, g.getMaxStrLen(g.COLUMNS.checksum()));
        assertEquals(30, g.getMaxStrLen(g.COLUMNS.id()));
        assertEquals(-1, g.getMaxStrLen(g.COLUMNS.message()));
        g.reset();
        g.init();
        g.clear();
        g.close();
    }

    @Test
    public void logCursorIsCallable() {
        LogCursor l = new LogCursor(cc());
        assertEquals("log", l.meta().getName());

        GrainsCursor gc = new GrainsCursor(cc());

        l.orderBy(l.COLUMNS.userid().asc(),
                  l.COLUMNS.pkvalue3().desc(),
                  l.COLUMNS.pkvalue2());
        assertAll(
                // Unknown column
                () -> assertThrows(CelestaException.class,
                        () -> l.orderBy(l.COLUMNS.userid(), gc.COLUMNS.message(), l.COLUMNS.pkvalue2())),
                // Column repetition
                () -> assertThrows(CelestaException.class,
                        () -> l.orderBy(l.COLUMNS.userid().asc(),
                                        l.COLUMNS.pkvalue3().desc(),
                                        l.COLUMNS.pkvalue3())),
                // empty orderBy
                () -> l.orderBy()
        );
    }

    @Test
    public void cursorsAreClosingOnContext() {
        BasicCursor a = new LogCursor(cc());
        BasicCursor b = new PermissionsCursor(cc());
        BasicCursor c = new RolesCursor(cc());
        BasicCursor d = new CalllogCursor(cc());
        assertFalse(a.isClosed());
        assertFalse(b.isClosed());
        assertFalse(c.isClosed());
        assertFalse(d.isClosed());

        cc().close();

        assertTrue(a.isClosed());
        assertTrue(b.isClosed());
        assertTrue(c.isClosed());
        assertTrue(d.isClosed());
    }

    @Test
    public void getMaxStrLenReturnsCorrectValues(){
        Simple_tableCursor simpleTableCursor = new Simple_tableCursor(cc());
        assertEquals(255, simpleTableCursor.getMaxStrLen(simpleTableCursor.COLUMNS.name()));
        assertEquals(-1, simpleTableCursor.getMaxStrLen(simpleTableCursor.COLUMNS.text_field()));
        Simple_viewCursor simpleViewCursor = new Simple_viewCursor(cc());
        //Note that this generally does not make sense:
        // we check the length of the field that belongs to other cursor.
        //However, we are covering a certain execution path here.
        assertEquals(-1, simpleTableCursor.getMaxStrLen(simpleViewCursor.COLUMNS.name()));
    }

    @Override
    protected String scorePath() {
        return "score";
    }
}

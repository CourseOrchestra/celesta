package ru.curs.celesta;

import org.junit.jupiter.api.Test;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.syscursors.*;

import static org.junit.jupiter.api.Assertions.*;

public class InitTest extends AbstractCelestaTest {


    @Test
    public void grainCursorIsCallable() {
        GrainsCursor g = new GrainsCursor(cc());
        assertEquals("grains", g.meta().getName());
        assertEquals(8, g.getMaxStrLen("checksum"));
        assertEquals(30, g.getMaxStrLen("id"));
        assertEquals(-1, g.getMaxStrLen("message"));
        g.reset();
        g.init();
        g.clear();
        g.close();
    }

    @Test
    public void logCursorIsCallable() {
        LogCursor l = new LogCursor(cc());
        assertEquals("log", l.meta().getName());
        l.orderBy("userid ASC", "pkvalue3 DESC", "pkvalue2");
        assertAll(
                // Неизвестная колонка
                () -> assertThrows(CelestaException.class,
                        () -> l.orderBy("userid", "psekvalue3 ASC", "pkvsealue3")),
                // Повтор колонок
                () -> assertThrows(CelestaException.class,
                        () -> l.orderBy("userid ASC", "pkvalue3 ASC", "pkvalue3 DESC")),
                // Пустой orderBy
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


    @Override
    protected String scorePath() {
        return "score";
    }
}

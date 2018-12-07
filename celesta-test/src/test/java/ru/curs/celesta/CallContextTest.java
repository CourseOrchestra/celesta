package ru.curs.celesta;

import org.junit.jupiter.api.Test;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.syscursors.GrainsCursor;
import ru.curs.celesta.syscursors.LogsetupCursor;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CallContextTest extends AbstractCelestaTest {
    public static final String USER_ID = "foo";

    @Override
    protected String scorePath() {
        return "score";
    }

    @Test
    void cursorsAreClosedWithCallContext() {
        GrainsCursor grainsCursor = new GrainsCursor(cc());
        LogsetupCursor logSetupCursor = new LogsetupCursor(cc());

        assertAll(
                () -> assertFalse(cc().isClosed()),
                () -> assertFalse(grainsCursor.isClosed()),
                () -> assertFalse(logSetupCursor.isClosed()),
                () -> assertEquals(2, cc().getDataAccessorsCount())
        );

        cc().close();

        assertAll(
                () -> assertTrue(cc().isClosed()),
                () -> assertTrue(grainsCursor.isClosed()),
                () -> assertTrue(logSetupCursor.isClosed()),
                () -> assertEquals(0, cc().getDataAccessorsCount())
        );
    }

    @Test
    void failsIfTooManyCursorsAreCreated() {
        for (int i = 0; i < CallContext.MAX_DATA_ACCESSORS + 1; i++) {
            new GrainsCursor(cc());
        }
        assertThrows(CelestaException.class,
                () -> new GrainsCursor(cc()));
    }

    @Test
    void chainOfCursorsShortensOnCursorClose() {
        BasicCursor[] cursors = new BasicCursor[6];

        for (int i = 0; i < cursors.length; i++) {
            cursors[i] = new GrainsCursor(cc());
            assertEquals(i + 1, cc().getDataAccessorsCount());
            assertSame(cursors[i], cc().getLastDataAccessor());
        }

        for (int i = 1; i < 3; i++) {
            cursors[i * 2 - 1].close();
            assertEquals(cursors.length - i, cc().getDataAccessorsCount());
        }

        cc().close();
        assertEquals(0, cc().getDataAccessorsCount());
        assertNull(cc().getLastDataAccessor());

        cc().close();
    }

    @Test
    void closeAndCreateLastCursorInChain() {
        BasicCursor[] cursors = new BasicCursor[3];

        for (int i = 0; i < cursors.length; i++) {
            cursors[i] = new GrainsCursor(cc());
            assertEquals(i + 1, cc().getDataAccessorsCount());
            assertSame(cursors[i], cc().getLastDataAccessor());
        }

        cursors[cursors.length - 1].close();
        assertEquals(cursors.length - 1, cc().getDataAccessorsCount());
        assertSame(cursors[cursors.length - 2], cc().getLastDataAccessor());

        cursors[cursors.length - 1] = new GrainsCursor(cc());
        assertEquals(cursors.length, cc().getDataAccessorsCount());
        assertSame(cursors[cursors.length - 1], cc().getLastDataAccessor());
    }

    @Test
    void notActiveContextCanBeClosed() {
        CallContext ctx = new CallContext(USER_ID);
        assertEquals(USER_ID, ctx.getUserId());
        assertFalse(ctx.isClosed());
        ctx.close();
        assertTrue(ctx.isClosed());
    }

    @Test
    void contextCanBeClosedMultipleTimes() {
        CallContext ctx = cc();
        assertFalse(ctx.isClosed());
        ctx.close();
        ctx.close();
        assertTrue(ctx.isClosed());
    }

    @Test
    void activeContextCannotBeActivated() {
        CallContext ctx = cc();
        assertThrows(CelestaException.class, () ->
                ctx.activate(ctx.getCelesta(), "foo"));
    }

    @Test
    void nonActiveContextCanBeRolledBack() {
        CallContext ctx = new CallContext(USER_ID);
        ctx.rollback();

        ctx = cc();
        ctx.close();
        ctx.rollback();
    }

    @Test
    void nonActiveContextCannotBeCommitted() {
        CallContext ctx = new CallContext(USER_ID);
        assertThrows(CelestaException.class, () -> ctx.commit());
    }

    @Test
    void closedContextCannotBeCommitted() {
        CallContext ctx = cc();
        ctx.close();
        assertThrows(CelestaException.class, () -> ctx.commit());
    }

    @Test
    void activeContextCanBeCommitedAndRolledBack() {
        CallContext ctx = cc();
        ctx.commit();
        ctx.rollback();
    }

    @Test
    void durationIsMeasuredForActiveContext() throws InterruptedException {
        CallContext activeCtx = cc();
        CallContext voidCtx = new CallContext(USER_ID);
        Thread.sleep(1);
        assertTrue(activeCtx.getDurationNs() > 1000);
        assertEquals(0, voidCtx.getDurationNs());
        activeCtx.close();
        long d = activeCtx.getDurationNs();
        Thread.sleep(1);
        assertEquals(d, activeCtx.getDurationNs());
    }

    @Test
    void dbPidIsCalculatedForActiveContext(){
        CallContext activeCtx = cc();
        CallContext voidCtx = new CallContext(USER_ID);
        assertTrue(activeCtx.getDBPid() != 0);
        assertEquals(0, voidCtx.getDBPid());
    }
}

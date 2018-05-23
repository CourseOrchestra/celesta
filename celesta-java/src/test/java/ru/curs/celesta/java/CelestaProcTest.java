package ru.curs.celesta.java;

import org.junit.jupiter.api.Test;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.SessionContext;
import ru.curs.celesta.syscursors.UserrolesCursor;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

public class CelestaProcTest extends AbstractCelestaTest {

    private static final String barQualifier = "ru.curs.celesta.java.annotated.bar.Bar#annotatedBarMethod";
    private static final String fooQualifier = "ru.curs.celesta.java.annotated.foo.Foo#annotatedFooMethod";
    private static final String noReturnValueQualifier = "ru.curs.celesta.java.annotated.returnvoid.ReturnVoid#noReturnValue";
    private static final String callContextInjectionQualifier = "ru.curs.celesta.java.callcontext.CallContextInjection#run";

    @Test
    void testBarMethod() {
        Object result = this.celesta.get().runProc(SessionContext.SYSTEM_SESSION_ID, barQualifier, "qwe", 2);

        assertAll(
                () -> assertTrue(result instanceof String),
                () -> assertEquals(
                        SessionContext.SYSTEM_SESSION_ID + "qwe" + SessionContext.SYSTEM_SESSION_ID + "qwe",
                        result
                )
        );
    }

    @Test
    void testFooMethod() {
        Object result = celesta.get().runProc(SessionContext.SYSTEM_SESSION_ID, fooQualifier, 5, 2);

        assertAll(
                () -> assertTrue(result instanceof Integer),
                () -> assertEquals(
                        7,
                        result
                )
        );
    }

    @Test
    void testNoReturnValueMethod() {
        Object result = celesta.get().runProc(SessionContext.SYSTEM_SESSION_ID, noReturnValueQualifier, 5);
        assertNull(result);
    }

    @Test
    void testCelestaExceptionOnNotExistedProcedure() {
        assertThrows(
                CelestaException.class,
                () -> celesta.get().runProc(SessionContext.SYSTEM_SESSION_ID, "ru.curs.Qf#noSuchMethod")
        );
    }

    @Test
    void testCallContextInjection() {
        AtomicBoolean isAssertionExecuted = new AtomicBoolean(false);
        Consumer<CallContext> callContextConsumer = callContext -> {
            assertNotNull(callContext);
            isAssertionExecuted.set(true);
        };

        celesta.get().runProc(SessionContext.SYSTEM_SESSION_ID, callContextInjectionQualifier, callContextConsumer);

        assertTrue(isAssertionExecuted.get());
    }

    @Test
    void testRunProcAsync() throws Exception {

        Instant start = Instant.now();
        Future<Object> resultFuture = celesta.get()
                .runProcAsync(SessionContext.SYSTEM_SESSION_ID, barQualifier, 500, "qwe", 2);
        Object result = resultFuture.get();
        Instant end = Instant.now();

        assertAll(
                () -> assertTrue(Duration.between(start, end).toMillis() >= 500),
                () -> assertTrue(result instanceof String),
                () -> assertEquals(
                        SessionContext.SYSTEM_SESSION_ID + "qwe" + SessionContext.SYSTEM_SESSION_ID + "qwe",
                        result
                )
        );
    }


    @Test
    void testRunProcAsyncAfterLogout() throws Exception {
        AtomicReference<UserrolesCursor> cursorAtomicReference = new AtomicReference<>();
        Consumer<CallContext> callContextConsumer = callContext -> {
            UserrolesCursor cursor = new UserrolesCursor(callContext);
            cursor.setFilter("userid", "'super'");
            cursor.first();
            cursorAtomicReference.compareAndSet(null, cursor);
        };

        Future<Object> resultFuture = celesta.get().runProcAsync(
                SessionContext.SYSTEM_SESSION_ID, callContextInjectionQualifier, 500, callContextConsumer
        );
        this.celesta.get().logout(SessionContext.SYSTEM_SESSION_ID, false);
        resultFuture.get();

        UserrolesCursor c = cursorAtomicReference.get();
        assertEquals("super", c.getUserid());
        assertEquals("editor", c.getRoleid());
        assertNotEquals(SessionContext.SYSTEM_SESSION_ID, c.callContext().getSessionId());
    }
}

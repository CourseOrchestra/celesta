package ru.curs.celesta.java;

import org.junit.jupiter.api.Test;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.SessionContext;
import ru.curs.celesta.java.annotated.bar.Bar;
import ru.curs.celesta.java.callcontext.CallContextInjection;
import ru.curs.celesta.syscursors.UserrolesCursor;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

public class CelestaRunTest extends AbstractCelestaTest {

    @Test
    void testBarMethod() {
        Bar bar = new Bar();
        String result = this.celesta.get()
                .run(
                        SessionContext.SYSTEM_SESSION_ID,
                        context -> bar.annotatedBarMethod(context, "qwe", 2)
                );

        assertAll(
                () -> assertTrue(result instanceof String),
                () -> assertEquals(
                        SessionContext.SYSTEM_SESSION_ID + "qwe" + SessionContext.SYSTEM_SESSION_ID + "qwe",
                        result
                )
        );
    }

    @Test
    void testCallContextInjection() {
        AtomicBoolean isAssertionExecuted = new AtomicBoolean(false);
        Consumer<CallContext> callContextConsumer = callContext -> {
            assertNotNull(callContext);
            isAssertionExecuted.set(true);
        };

        CallContextInjection callContextInjection = new CallContextInjection();
        this.celesta.get().run(
                SessionContext.SYSTEM_SESSION_ID,
                context -> callContextInjection.run(context, callContextConsumer)
        );

        assertTrue(isAssertionExecuted.get());
    }

    @Test
    void testRunAsync() throws Exception {

        Bar bar = new Bar();
        Instant start = Instant.now();
        Future<Object> resultFuture = celesta.get()
                .runAsync(
                        SessionContext.SYSTEM_SESSION_ID,
                        context -> bar.annotatedBarMethod(context, "qwe", 2), 500);
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
    void testRunAsyncAfterLogout() throws Exception {
        AtomicReference<UserrolesCursor> cursorAtomicReference = new AtomicReference<>();
        Consumer<CallContext> callContextConsumer = callContext -> {
            UserrolesCursor cursor = new UserrolesCursor(callContext);
            cursor.setFilter("userid", "'super'");
            cursor.first();
            cursorAtomicReference.compareAndSet(null, cursor);
        };


        CallContextInjection callContextInjection = new CallContextInjection();
        Future<Void> resultFuture = this.celesta.get().runAsync(
                SessionContext.SYSTEM_SESSION_ID,
                context -> callContextInjection.run(context, callContextConsumer),
                500
        );
        this.celesta.get().logout(SessionContext.SYSTEM_SESSION_ID, false);
        resultFuture.get();

        UserrolesCursor c = cursorAtomicReference.get();
        assertEquals("super", c.getUserid());
        assertEquals("editor", c.getRoleid());
        assertNotEquals(SessionContext.SYSTEM_SESSION_ID, c.callContext().getSessionId());
    }
}

package ru.curs.celesta;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

public class CelestaProcTest {

    private static final String barQualifier = "ru.curs.celesta.annotated.bar.Bar#annotatedBarMethod";
    private static final String fooQualifier = "ru.curs.celesta.annotated.foo.Foo#annotatedFooMethod";
    private static final String noReturnValueQualifier = "ru.curs.celesta.annotated.returnvoid.ReturnVoid#noReturnValue";
    private static final String callContextInjectionQualifier = "ru.curs.celesta.callcontext.CallContextInjection#run";

    private Optional<Celesta> celesta;


    @BeforeEach
    void setUp() {
        Properties params = new Properties();
        params.setProperty(
                "score.path",
                "src" + File.separator + "test" + File.separator + "resources" + File.separator + "emptyScore"
        );
        params.setProperty("h2.in-memory", "true");
        params.setProperty("celestaScan", "ru.curs.celesta.annotated, ru.curs.celesta.callcontext");
        this.celesta = Optional.of(Celesta.createInstance(params));

        this.celesta.get().login(SessionContext.SYSTEM_SESSION_ID, SessionContext.SYSTEM_USER_ID);
    }

    @AfterEach
    void tearDown() {
        this.celesta.ifPresent(this::closeCelesta);
    }

    private void closeCelesta(Celesta celesta) {
        try {
            celesta.connectionPool.get().createStatement().execute("SHUTDOWN");
            celesta.close();
        } catch (Exception e) {
            throw new CelestaException(e);
        }
    }


    @Test
    void testBarMethod() {
        Object result = celesta.get().runProc(SessionContext.SYSTEM_SESSION_ID, barQualifier, "qwe", 2);

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
                .runProcAsync(SessionContext.SYSTEM_SESSION_ID, fooQualifier, 500, 5, 2);
        Object result = resultFuture.get();
        Instant end = Instant.now();

        assertAll(
                () -> assertTrue(Duration.between(start, end).toMillis() >= 500),
                () -> assertTrue(result instanceof Integer),
                () -> assertEquals(
                        7,
                        result
                )
        );
    }

}

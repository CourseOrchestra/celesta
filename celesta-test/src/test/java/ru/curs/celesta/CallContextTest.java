package ru.curs.celesta;

import org.junit.jupiter.api.*;
import ru.curs.celesta.syscursors.GrainsCursor;
import ru.curs.celesta.syscursors.LogsetupCursor;

import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CallContextTest {

    private static Celesta celesta;

    private PySessionContext sc = new PySessionContext("super", "foo");
    private CallContext context;

    @BeforeAll
    public static void init() {
        Properties properties = new Properties();
        properties.setProperty("score.path", "score");
        properties.setProperty("h2.in-memory", "true");

        celesta = Celesta.createInstance(properties);
    }

    @AfterAll
    public static void destroy() throws SQLException {
        celesta.callContext(new PySessionContext("super", "foo")).getConn().createStatement().execute("SHUTDOWN");
        celesta.close();
    }

    @BeforeEach
    public void before() {
        context = celesta.callContext(sc);
    }

    @AfterEach
    public void after() {
        context.close();
    }

    @Test
    public void testClose() {
        GrainsCursor grainsCursor = new GrainsCursor(context);
        LogsetupCursor logSetupCursor = new LogsetupCursor(context);

        assertAll(
                () -> assertFalse(context.isClosed()),
                () -> assertFalse(grainsCursor.isClosed()),
                () -> assertFalse(logSetupCursor.isClosed())
        );

        context.close();

        assertAll(
                () -> assertTrue(context.isClosed()),
                () -> assertTrue(grainsCursor.isClosed()),
                () -> assertTrue(logSetupCursor.isClosed())
        );
    }

}

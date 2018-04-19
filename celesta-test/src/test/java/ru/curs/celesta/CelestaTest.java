package ru.curs.celesta;


import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static  org.junit.jupiter.api.Assertions.*;
import org.python.core.Py;
import org.python.core.PyObject;
import ru.curs.celesta.syscursors.UserrolesCursor;

import java.util.Properties;
import java.util.concurrent.Future;

public class CelestaTest {

    private static Celesta celesta;
    private static final String USER_ID = "super";
    private static final String ROLE_ID = "editor";

    @BeforeAll
    static void beforeAll() throws CelestaException {
        Properties properties = new Properties();
        properties.put("score.path", "testScore");
        properties.put("h2.in-memory", "true");
        celesta = Celesta.createInstance(properties);
    }

    @AfterAll
    static void afterAll() throws Exception {
        celesta.connectionPool.get().createStatement().execute("SHUTDOWN");
        celesta.close();
    }


    @Test
    void testRunPythonAsync() throws Exception {
        String sesId = "debug";
        celesta.login(sesId, USER_ID);
        Future<PyObject> future = celesta.runPythonAsync(sesId, "gtest.async.execute", 100);

        UserrolesCursor c = Py.tojava(future.get(), UserrolesCursor.class);

        assertEquals(USER_ID, c.getUserid());
        assertEquals(ROLE_ID, c.getRoleid());
        assertEquals(sesId,   c.callContext().getSessionId()
        );
    }

    @Test
    void testRunPythonAsyncAfterLogout() throws Exception {
        String sesId = "debug";
        celesta.login(sesId, USER_ID);
        Future<PyObject> future = celesta.runPythonAsync(sesId, "gtest.async.execute", 500);
        celesta.logout(sesId, false);

        UserrolesCursor c = Py.tojava(future.get(), UserrolesCursor.class);

        assertEquals(USER_ID, c.getUserid());
        assertEquals(ROLE_ID, c.getRoleid());
        assertNotEquals(sesId,   c.callContext().getSessionId());
    }


}

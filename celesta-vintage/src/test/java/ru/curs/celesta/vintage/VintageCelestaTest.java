package ru.curs.celesta.vintage;

import org.junit.jupiter.api.AfterAll;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.SessionContext;
import ru.curs.celesta.java.annotation.CelestaProc;
import ru.curs.celesta.vintage.java.JavaTableCursor;

import java.io.File;
import java.util.Optional;
import java.util.Properties;

public class VintageCelestaTest {

    private static Optional<Celesta> celesta;

    @BeforeAll
    static void beforeAll() {
        String scorePrefix = "src" + File.separator + "test" + File.separator
                + "resources" + File.separator + "ru" + File.separator + "curs"
                + File.separator + "celesta" + File.separator + "vintage" + File.separator;

        Properties params = new Properties();
        params.setProperty("score.path", scorePrefix + "jythonScore");
        params.setProperty("score.java.path", scorePrefix + "javaScore");
        params.setProperty("h2.in-memory", "true");
        params.setProperty("pylib.path", "../celesta-test/pylib");
        params.setProperty("celestaScan", "ru.curs.celesta.vintage");
        celesta = Optional.of(Celesta.createInstance(params));
        celesta.get().login(SessionContext.SYSTEM_SESSION_ID, SessionContext.SYSTEM_USER_ID);
        celesta.get().javaLogin(SessionContext.SYSTEM_SESSION_ID, SessionContext.SYSTEM_USER_ID);
    }

    @AfterAll
    public static void destroy() {
        celesta.ifPresent(c -> {
            try {
                c.callContext().getConn().createStatement().execute("SHUTDOWN");
                c.close();
            } catch (Exception e) {
                throw new CelestaException(e);
            }
        });
    }

    @Test
    void testJythonScore() {
        celesta.get().runPython(
                SessionContext.SYSTEM_SESSION_ID,
                "jython.jython.testJythonTable"
        );
    }

    @Test
    void testJavaScore() {
        celesta.get().runProc(
                SessionContext.SYSTEM_SESSION_ID,
                "ru.curs.celesta.vintage.VintageCelestaTest$Procedures#testJavaTable"
        );
    }


    public static class Procedures {

        @CelestaProc
        public void testJavaTable(CallContext context) {
            JavaTableCursor cursor = new JavaTableCursor(context);
            assertEquals(0, cursor.count());

            cursor.setVal(5);
            cursor.insert();
            cursor.clear();
            assertEquals(1, cursor.count());

            cursor.first();
            assertAll(
                    () -> assertEquals(Integer.valueOf(1), cursor.getId()),
                    () -> assertEquals(Integer.valueOf(5), cursor.getVal())
            );
        }
    }

}

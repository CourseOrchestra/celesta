package ru.curs.celesta;

import org.junit.jupiter.api.*;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertSame;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractCelestaTest {

    private final Celesta celesta;

    {
        Properties params = new Properties();
        params.setProperty("score.path", scorePath());
        params.setProperty("h2.in-memory", "true");
        celesta = Celesta.createInstance(params);

        assertSame(celesta.getSetupProperties(), params);
    }

    private CallContext cc;

    protected abstract String scorePath();

    public CallContext cc() {
        return cc;
    }


    @AfterAll
    void tearDown() {
        try {
            try (CallContext cc = new SystemCallContext(celesta)) {
                cc.getConn().createStatement().execute("SHUTDOWN");
            }
            celesta.close();
        } catch (Exception e) {
            throw new CelestaException(e);
        }
    }

    @BeforeEach
    final void beforeEach(TestInfo ti) {
        cc = new SystemCallContext(celesta, ti.getDisplayName());
    }

    @AfterEach
    public void after() {
        cc.close();
    }
}

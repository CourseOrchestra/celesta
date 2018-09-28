package ru.curs.celesta;

import org.junit.jupiter.api.*;
import ru.curs.celesta.syscursors.GrainsCursor;
import ru.curs.celesta.syscursors.LogsetupCursor;

import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class CallContextTest extends AbstractCelestaTest {

    @Override
    protected String scorePath() {
        return "score";
    }

    @Test
    public void cursorsAreClosedWithCallContext() {
        GrainsCursor grainsCursor = new GrainsCursor(cc());
        LogsetupCursor logSetupCursor = new LogsetupCursor(cc());

        assertAll(
                () -> assertFalse(cc().isClosed()),
                () -> assertFalse(grainsCursor.isClosed()),
                () -> assertFalse(logSetupCursor.isClosed())
        );

        cc().close();

        assertAll(
                () -> assertTrue(cc().isClosed()),
                () -> assertTrue(grainsCursor.isClosed()),
                () -> assertTrue(logSetupCursor.isClosed())
        );
    }

    @Test
    public void failsIfTooManyCursorsAreCreated() {
        for (int i = 0; i < CallContext.MAX_DATA_ACCESSORS + 1; i++) {
            new GrainsCursor(cc());
        }
        assertThrows(CelestaException.class,
                () -> new GrainsCursor(cc()));
    }

}

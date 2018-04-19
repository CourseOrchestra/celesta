package ru.curs.celesta.py;

import org.python.core.PyFunction;
import ru.curs.celesta.Celesta;
import ru.curs.celesta.dbutils.Cursor;
import ru.curs.celesta.event.TriggerType;

@SuppressWarnings("unused")
public final class PyTriggerRegistrationAdaptor {

    private PyTriggerRegistrationAdaptor() {
        throw new AssertionError();
    }

    public static void registerTrigger(
            Celesta celesta, TriggerType triggerType, PyFunction pyFunction, Class<Cursor> cursorClass
    ) {
        celesta.getTriggerDispatcher().registerTrigger(
                triggerType,
                cursorClass,
                cursor -> {
                    Object[] args = {cursor};
                    pyFunction._jcall(args);
                }
        );
    }
}

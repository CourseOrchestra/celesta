package ru.curs.celesta.mock;

import org.python.core.PyObject;
import ru.curs.celesta.*;
import ru.curs.celesta.event.TriggerDispatcher;
import ru.curs.celesta.score.Score;

import java.util.Properties;
import java.util.concurrent.Future;

public class CelestaImpl implements PyCelesta {

    private final TriggerDispatcher triggerDispatcher = new TriggerDispatcher();

    @Override
    public TriggerDispatcher getTriggerDispatcher() {
        return this.triggerDispatcher;
    }

    @Override
    public PyObject runPython(String sesId, String proc, Object... param) throws CelestaException {
        return null;
    }

    @Override
    public PyObject runPython(String sesId, CelestaMessage.MessageReceiver rec, ShowcaseContext sc, String proc, Object... param) throws CelestaException {
        return null;
    }

    @Override
    public Future<PyObject> runPythonAsync(String sesId, String proc, long delay, Object... param) {
        return null;
    }

    @Override
    public Score getScore() {
        return null;
    }

    @Override
    public Properties getSetupProperties() {
        return null;
    }
}

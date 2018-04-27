package ru.curs.celesta.mock;

import org.python.core.PyObject;
import ru.curs.celesta.*;
import ru.curs.celesta.dbutils.LoggingManager;
import ru.curs.celesta.dbutils.PermissionManager;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.event.TriggerDispatcher;
import ru.curs.celesta.score.Score;

import java.util.Properties;
import java.util.concurrent.Future;

public class CelestaImpl implements PyCelesta {

    private final TriggerDispatcher triggerDispatcher = new TriggerDispatcher();

    private final DBAdaptor dbAdaptor;
    private final ConnectionPool connectionPool;
    private final Score score;
    private final PermissionManager permissionManager;
    private final LoggingManager loggingManager;

    public CelestaImpl(DBAdaptor dbAdaptor, ConnectionPool connectionPool, Score score) {
        this.dbAdaptor = dbAdaptor;
        this.connectionPool = connectionPool;
        this.score = score;
        this.permissionManager = new PermissionManager(this, dbAdaptor);
        this.loggingManager = new LoggingManager(this, dbAdaptor);
    }

    @Override
    public TriggerDispatcher getTriggerDispatcher() {
        return this.triggerDispatcher;
    }

    @Override
    public CallContext callContext() {
        return PyCallContext.builder()
                .setSesContext(getSystemSessionContext())
                .setCelesta(this)
                .setDbAdaptor(this.dbAdaptor)
                .setConnectionPool(this.connectionPool)
                .setScore(this.score)
                .setPermissionManager(this.permissionManager)
                .setLoggingManager(this.loggingManager)
                .createCallContext();
    }

    @Override
    public PySessionContext getSystemSessionContext() {
        return Celesta.SYSTEM_SESSION;
    }

    @Override
    public PyObject runPython(String sesId, String proc, Object... param) {
        return null;
    }

    @Override
    public PyObject runPython(String sesId, CelestaMessage.MessageReceiver rec, ShowcaseContext sc, String proc, Object... param) {
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

    public PermissionManager getPermissionManager() {
        return permissionManager;
    }

    public LoggingManager getLoggingManager() {
        return loggingManager;
    }
}

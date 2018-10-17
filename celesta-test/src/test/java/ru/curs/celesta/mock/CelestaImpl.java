package ru.curs.celesta.mock;

import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.ICelesta;
import ru.curs.celesta.dbutils.IProfiler;
import ru.curs.celesta.dbutils.LoggingManager;
import ru.curs.celesta.dbutils.PermissionManager;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.event.TriggerDispatcher;
import ru.curs.celesta.score.Score;

import java.util.Properties;

public class CelestaImpl implements ICelesta {

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
    public Score getScore() {
        return score;
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

    @Override
    public ConnectionPool getConnectionPool() {
        return connectionPool;
    }

    @Override
    public IProfiler getProfiler() {
        //do-nothing mock profiler
        return context -> {
        };
    }

    @Override
    public DBAdaptor getDBAdaptor() {
        return dbAdaptor;
    }

    public void close() throws Exception {
        connectionPool.close();
    }

    public boolean isClosed() {
        return connectionPool.isClosed();
    }

}

package ru.curs.celesta.test.mock;

import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.ICelesta;
import ru.curs.celesta.dbutils.IProfiler;
import ru.curs.celesta.dbutils.LoggingManager;
import ru.curs.celesta.dbutils.PermissionManager;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.event.TriggerDispatcher;
import ru.curs.celesta.score.Score;

import java.util.Properties;

/**
 * Mock Celesta instance.
 */
public final class CelestaImpl implements ICelesta {

    private final TriggerDispatcher triggerDispatcher = new TriggerDispatcher();

    private final DBAdaptor dbAdaptor;
    private final ConnectionPool connectionPool;
    private final Score score;
    private final String scorePath;
    private final PermissionManager permissionManager;
    private final LoggingManager loggingManager;

    public CelestaImpl(DBAdaptor dbAdaptor, ConnectionPool connectionPool, Score score) {
        this(dbAdaptor, connectionPool, score, null);
    }

    public CelestaImpl(DBAdaptor dbAdaptor, ConnectionPool connectionPool, Score score, String scorePath) {
        this.dbAdaptor = dbAdaptor;
        this.connectionPool = connectionPool;
        this.score = score;
        this.scorePath = scorePath;
        this.permissionManager = new PermissionManager(this);
        this.loggingManager = new LoggingManager(this);
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

    @Override
    public PermissionManager getPermissionManager() {
        return permissionManager;
    }

    @Override
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

    @Override
    public void close() {
        connectionPool.close();
    }

    /**
     * Whether connection pool is closed.
     *
     * @return
     */
    public boolean isClosed() {
        return connectionPool.isClosed();
    }

    /**
     * Returns Celesta's score path.
     *
     * @return
     */
    public String getScorePath() {
        return scorePath;
    }

}

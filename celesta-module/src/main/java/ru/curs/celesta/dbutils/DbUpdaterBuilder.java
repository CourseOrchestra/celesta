package ru.curs.celesta.dbutils;

import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.ICelesta;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.score.Score;

public class DbUpdaterBuilder {
    private DBAdaptor dbAdaptor;
    private ConnectionPool connectionPool;
    private Score score;
    private boolean forceDdInitialize;
    private ICelesta celesta;
    private PermissionManager permissionManager;
    private LoggingManager loggingManager;

    public DbUpdaterBuilder dbAdaptor(DBAdaptor dbAdaptor) {
        this.dbAdaptor = dbAdaptor;
        return this;
    }

    public DbUpdaterBuilder connectionPool(ConnectionPool connectionPool) {
        this.connectionPool = connectionPool;
        return this;
    }

    public DbUpdaterBuilder score(Score score) {
        this.score = score;
        return this;
    }

    public DbUpdaterBuilder forceDdInitialize(boolean forceDdInitialize) {
        this.forceDdInitialize = forceDdInitialize;
        return this;
    }

    public DbUpdaterBuilder setCelesta(ICelesta celesta) {
        this.celesta = celesta;
        return this;
    }

    public DbUpdaterBuilder setPermissionManager(PermissionManager permissionManager) {
        this.permissionManager = permissionManager;
        return this;
    }

    public DbUpdaterBuilder setLoggingManager(LoggingManager loggingManager) {
        this.loggingManager = loggingManager;
        return this;
    }

    public DbUpdaterImpl build() {
        return new DbUpdaterImpl(connectionPool, score, forceDdInitialize,
                dbAdaptor, celesta, permissionManager, loggingManager);
    }
}

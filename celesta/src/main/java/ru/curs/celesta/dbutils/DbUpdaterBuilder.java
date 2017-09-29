package ru.curs.celesta.dbutils;

import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.score.Score;

public class DbUpdaterBuilder {
    private DBAdaptor dbAdaptor;
    private ConnectionPool connectionPool;
    private Score score;
    private boolean forceDdInitialize;

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

    public DbUpdater build() {
        return new DbUpdater(connectionPool, score, forceDdInitialize, dbAdaptor);
    }
}

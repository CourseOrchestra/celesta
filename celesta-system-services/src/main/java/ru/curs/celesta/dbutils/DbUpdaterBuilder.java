package ru.curs.celesta.dbutils;

import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.ICelesta;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.score.Score;

/**
 * DB updater builder.
 */
public final class DbUpdaterBuilder {
    private DBAdaptor dbAdaptor;
    private ConnectionPool connectionPool;
    private Score score;
    private boolean forceDdInitialize;
    private ICelesta celesta;

    /**
     * Sets a DB adaptor.
     *
     * @param dbAdaptor  adaptor of concrete DB.
     * @return {@code this}
     */
    public DbUpdaterBuilder dbAdaptor(DBAdaptor dbAdaptor) {
        this.dbAdaptor = dbAdaptor;
        return this;
    }

    /**
     * Sets connection pool.
     *
     * @param connectionPool  connection pool
     * @return {@code this}
     */
    public DbUpdaterBuilder connectionPool(ConnectionPool connectionPool) {
        this.connectionPool = connectionPool;
        return this;
    }

    /**
     * Sets score.
     *
     * @param score  score
     * @return {@code this}
     */
    public DbUpdaterBuilder score(Score score) {
        this.score = score;
        return this;
    }

    /**
     * Sets if DB initialization should be forced.
     *
     * @param forceDdInitialize  {@code true} - DB initialization should be forced
     *                           {@code false} - don't force DB initialization
     * @return {@code this}
     */
    public DbUpdaterBuilder forceDdInitialize(boolean forceDdInitialize) {
        this.forceDdInitialize = forceDdInitialize;
        return this;
    }

    /**
     * Sets Celesta instance.
     *
     * @param celesta
     * @return {@code this}
     */
    public DbUpdaterBuilder setCelesta(ICelesta celesta) {
        this.celesta = celesta;
        return this;
    }

    /**
     * Builds DB updater instance.
     *
     * @return
     */
    public DbUpdaterImpl build() {
        return new DbUpdaterImpl(connectionPool, score, forceDdInitialize,
                dbAdaptor, celesta);
    }

}

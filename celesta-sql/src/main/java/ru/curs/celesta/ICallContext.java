package ru.curs.celesta;

import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.score.AbstractScore;

import java.sql.Connection;

public interface ICallContext extends AutoCloseable {

    /**
     * Connection with database.
     *
     * @return
     */
    Connection getConn();

    /**
     * Whether the context was closed.
     *
     * @return
     */
    boolean isClosed();

    DBAdaptor getDbAdaptor();

    /**
     * Returns Celesta score.
     *
     * @return
     */
    AbstractScore getScore();

    @Override
    void close();
}

package ru.curs.celesta;

import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.score.AbstractScore;

import java.sql.Connection;

/**
 * It's a base interface of call context.
 * Its implementations allow to execute db operations in one transaction
 * and provides methods to access base units of celesta.
 */
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

    /**
     * Returns actual implementation of {@link DBAdaptor}
     *
     * @return
     */
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

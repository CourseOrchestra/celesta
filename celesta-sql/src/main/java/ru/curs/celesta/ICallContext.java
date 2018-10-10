package ru.curs.celesta;

import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.score.AbstractScore;

import java.sql.Connection;

public interface ICallContext extends AutoCloseable {

    /**
     * Connection with database
     */
    Connection getConn();

    /**
     * Whether the context was closed.
     */
    boolean isClosed();

    DBAdaptor getDbAdaptor();

    AbstractScore getScore();

    @Override
    void close();
}

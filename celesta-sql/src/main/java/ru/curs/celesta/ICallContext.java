package ru.curs.celesta;

import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.score.AbstractScore;

import java.sql.Connection;

public interface ICallContext extends AutoCloseable {

    /**
     * Соединение с базой данных.
     */
    Connection getConn();

    /**
     * Был ли контекст закрыт.
     */
    boolean isClosed();

    DBAdaptor getDbAdaptor();

    AbstractScore getScore();

    void close();
}

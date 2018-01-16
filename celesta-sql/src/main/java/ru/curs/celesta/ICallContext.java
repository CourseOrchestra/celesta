package ru.curs.celesta;

import ru.curs.celesta.dbutils.CsqlBasicDataAccessor;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;

import java.sql.Connection;

//TODO::Rename
public interface ICallContext<T extends CsqlBasicDataAccessor> extends AutoCloseable {

    /**
     * Duplicates callcontext with another JDBC connection.
     *
     * @throws CelestaException
     *             cannot create adaptor
     */
    ICallContext getCopy() throws CelestaException;

    /**
     * Соединение с базой данных.
     */
    Connection getConn();

    /**
     * Коммитит транзакцию.
     *
     * @throws CelestaException
     *             в случае проблемы с БД.
     */
    void commit() throws CelestaException;

    /**
     * Был ли контекст закрыт.
     */
    boolean isClosed();

    DBAdaptor getDbAdaptor();

    void close() throws CelestaException;
}

package ru.curs.celesta;

import ru.curs.celesta.dbutils.adaptors.DBAdaptor;

import java.sql.Connection;
import java.sql.SQLException;

public interface ConnectionPool extends AutoCloseable {
    Connection get();

    void setDbAdaptor(DBAdaptor dbAdaptor);

    @Override
    void close();

    boolean isClosed();

    /**
     * Executes 'commit' command on a connection without throwing a checked exception.
     *
     * @param conn  connection for commit execution.
     */
    default void commit(Connection conn) {
        try {
            if (conn != null) {
                conn.commit();
            }
        } catch (SQLException e) {
            throw new CelestaException(e);
        }
    }

}

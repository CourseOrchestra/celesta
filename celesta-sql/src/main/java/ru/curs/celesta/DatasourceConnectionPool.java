package ru.curs.celesta;

import ru.curs.celesta.dbutils.adaptors.DBAdaptor;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public final class DatasourceConnectionPool implements ConnectionPool {
    private final DataSource dataSource;
    private volatile boolean isClosed;

    public DatasourceConnectionPool(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public Connection get() {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            CelestaException celestaException =
                    new CelestaException("Could not connect to %s with error: %s", e.getMessage());
            celestaException.initCause(e);
            throw celestaException;
        }
    }

    @Override
    public void setDbAdaptor(DBAdaptor dbAdaptor) {
        //do nothing: connection validity is checked by the external pool
    }

    @Override
    public void close() {
        isClosed = true;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }
}

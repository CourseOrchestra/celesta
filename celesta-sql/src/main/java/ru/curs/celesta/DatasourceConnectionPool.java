package ru.curs.celesta;

import ru.curs.celesta.dbutils.adaptors.DBAdaptor;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

public final class DatasourceConnectionPool implements ConnectionPool {
    private final DataSource dataSource;
    private final AtomicBoolean isClosed = new AtomicBoolean();

    public DatasourceConnectionPool(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public Connection get() {
        if (isClosed.get()) {
            throw new CelestaException("ConnectionPool is closed");
        }
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            CelestaException celestaException =
                    new CelestaException("Could not connect to the database with error: %s",
                            e.getMessage());
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
        if (!isClosed.getAndSet(true)) {
            if (dataSource instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) dataSource).close();
                } catch (Exception e) {
                    throw new CelestaException(e);
                }
            }
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }
}

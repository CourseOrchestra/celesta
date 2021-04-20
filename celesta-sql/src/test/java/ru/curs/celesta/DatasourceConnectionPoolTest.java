package ru.curs.celesta;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.times;

class DatasourceConnectionPoolTest {


    DataSource ds = Mockito.mock(DataSource.class, Mockito.withSettings().extraInterfaces(Closeable.class));

    DatasourceConnectionPool pool = new DatasourceConnectionPool(ds);

    @Test
    void get() throws SQLException {
        Connection mockConn = Mockito.mock(Connection.class);
        Mockito.doReturn(mockConn).when(ds).getConnection();
        assertSame(mockConn, pool.get());
    }

    @Test
    void getFail() throws SQLException {
        SQLException exception = new SQLException();
        Mockito.doThrow(exception).when(ds).getConnection();

        assertSame(exception,
                assertThrows(CelestaException.class, pool::get).getCause());
    }


    @Test
    void close() throws IOException {
        pool.close();
        assertTrue(pool.isClosed());
        Mockito.verify((Closeable) ds, times(1)).close();
        assertThrows(CelestaException.class, pool::get);
    }

    @Test
    void repeatedClose() throws IOException {
        pool.close();
        pool.close();
        assertTrue(pool.isClosed());
        Mockito.verify((Closeable) ds, times(1)).close();
    }

    @Test
    void closeFail() throws IOException {
        IOException exception = new IOException();
        Mockito.doThrow(exception).when((Closeable) ds).close();
        assertSame(exception,
                assertThrows(CelestaException.class, pool::close).getCause());
    }
}
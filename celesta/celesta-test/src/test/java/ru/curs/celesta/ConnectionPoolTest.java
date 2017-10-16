package ru.curs.celesta;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ru.curs.celesta.dbutils.adaptors.configuration.DbAdaptorBuilder;

public class ConnectionPoolTest {

    private ConnectionPool connectionPool;

    @BeforeEach
    public void init() throws CelestaException {
        ConnectionPoolConfiguration cpc = new ConnectionPoolConfiguration();
        String jdbcUrl = "jdbc:h2:mem:celesta;DB_CLOSE_DELAY=-1";
        cpc.setJdbcConnectionUrl(jdbcUrl);
        cpc.setDriverClassName(AppSettings.resolveDbType(jdbcUrl).getDriverClassName());
        cpc.setLogin("");

        connectionPool = ConnectionPool.create(cpc);

        DbAdaptorBuilder dac = new DbAdaptorBuilder()
                .setDbType(AppSettings.DBType.H2)
                .setConnectionPool(connectionPool)
                .setH2ReferentialIntegrity(false);

        dac.createDbAdaptor();
    }


    @Test
    void testAllConnectionsSize() throws Exception {
        //Не забываем, что первый Connection получает dbAdaptor для выставления флага REFERENTIAL_INTEGRITY
        Connection conn1 = connectionPool.get();
        Connection conn2 = connectionPool.get();

        conn1.close();

        assertAll(
                () -> assertTrue(!conn1.isClosed()),
                () -> assertTrue(!conn2.isClosed()),
                () -> assertEquals(1, connectionPool.poolSize())
        );
    }


    @Test
    void testClosingConnectionPool() throws Exception {
        Connection conn1 = connectionPool.get();
        Connection conn2 = connectionPool.get();
        Connection conn3 = connectionPool.get();

        conn1.close();
        assertFalse(conn1.isClosed());
        connectionPool.close();
        conn3.close();
        assertAll(
                () -> assertTrue(conn1.isClosed()),
                () -> assertFalse(conn2.isClosed()),
                () -> assertTrue(conn3.isClosed()),
                () -> assertEquals(0, connectionPool.poolSize()),
                () -> assertTrue(connectionPool.isClosed())
        );
    }

    @Test
    void testThatConnectionPoolIsUnusableAfterClosing() throws Exception {
        Connection conn1 = connectionPool.get();
        Connection conn2 = connectionPool.get();
        Connection conn3 = connectionPool.get();

        conn1.close();

        connectionPool.close();

        assertThrows(CelestaException.class, () -> connectionPool.get());
        
        connectionPool.commit(conn2);
        conn3.close();
        
        assertAll(
                () -> assertTrue(conn1.isClosed()),
                () -> assertFalse(conn2.isClosed()),
                () -> assertTrue(conn3.isClosed()),
                () -> assertEquals(0, connectionPool.poolSize()),
                () -> assertTrue(connectionPool.isClosed())
        );
    }


    @AfterEach
    public void tearDown() {
        if (!connectionPool.isClosed()) {
            connectionPool.close();
        }
    }

}

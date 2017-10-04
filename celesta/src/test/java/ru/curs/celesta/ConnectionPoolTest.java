package ru.curs.celesta;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.curs.celesta.dbutils.adaptors.configuration.DbAdaptorBuilder;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.SQLException;

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
                () -> assertEquals(2, connectionPool.allConnections.size())
        );
    }


    @Test
    void testClosingConnectionPool() throws Exception {
        Connection conn1 = connectionPool.get();
        Connection conn2 = connectionPool.get();

        conn1.close();
        connectionPool.clear();

        assertAll(
                () -> assertTrue(conn1.isClosed()),
                () -> assertTrue(conn2.isClosed()),
                () -> assertEquals(0, connectionPool.allConnections.size()),
                () -> assertTrue(connectionPool.isClosed.get())
        );
    }

    @Test
    void testThatConnectionPoolIsUnusableAfterClosing() throws Exception {
        Connection conn1 = connectionPool.get();
        Connection conn2 = connectionPool.get();
        Connection conn3 = connectionPool.get();

        conn1.close();

        connectionPool.clear();

        assertThrows(CelestaException.class, () -> connectionPool.get());
        assertThrows(CelestaException.class, () -> connectionPool.commit(conn2));
        assertThrows(SQLException.class, () -> conn3.close());
    }


    @AfterEach
    public void tearDown() {
        if (!connectionPool.isClosed.get()) {
            connectionPool.clear();
        }
    }

}

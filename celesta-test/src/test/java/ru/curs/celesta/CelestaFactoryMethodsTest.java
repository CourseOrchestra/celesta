package ru.curs.celesta;

import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class CelestaFactoryMethodsTest {
    public static final String JDBC_URL = "jdbc:h2:mem:database;DB_CLOSE_DELAY=-1";
    private Properties properties;

    @BeforeEach
    void init() {
        properties = new Properties();
        properties.setProperty("score.path", "score");
        properties.setProperty("rdbms.connection.url", "jdbc:h2");
    }

    @Test
    void createInstanceWithDataSource() {
        DataSource dataSource = JdbcConnectionPool.create(JDBC_URL, "", "");
        verifyCelesta(Celesta.createInstance(properties, dataSource));
    }

    @Test
    void createInstanceWithConnectionPool() {
        ConnectionPool connectionPool = new DatasourceConnectionPool(
                 JdbcConnectionPool.create(JDBC_URL, "", ""));
        verifyCelesta(Celesta.createInstance(properties, connectionPool));
    }

    @Test
    void createInstanceWithPropertiesOnly() {
        properties.setProperty("h2.in-memory", "true");
        verifyCelesta(Celesta.createInstance(properties));
    }

    @Test
    void createInstanceWithDynamicallyLoadedProperties() {
        verifyCelesta(Celesta.createInstance());
    }

    private static void verifyCelesta(Celesta celesta){
        assertNotNull(celesta.getConnectionPool());
        assertEquals(DBType.H2, celesta.getDBAdaptor().getType());
    }
}

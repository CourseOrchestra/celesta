package ru.curs.celesta;

import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class H2MixedModeTest {

    private final String port = "6557";
    private final String jdbcURL = String.format(
            "jdbc:h2:tcp://localhost:%s/mem:celesta", port);

    @Test
    void h2InMemorySetupPropertyIsParsed() throws CelestaException {
        Properties params = new Properties();
        params.setProperty("score.path", "score");
        params.setProperty("h2.in-memory", "true");
        AppSettings as = new AppSettings(params);
        //embedded mode
        assertEquals("jdbc:h2:mem:celesta;DB_CLOSE_DELAY=-1", as.getDatabaseConnection());

        //mixed mode
        params.setProperty("h2.port", port);
        as = new AppSettings(params);
        assertEquals(Integer.parseInt(port), as.getH2Port());
        assertEquals(jdbcURL,
                as.getDatabaseConnection());

        params.setProperty("h2.port", "notanumber");
        assertThrows(CelestaException.class, () -> new AppSettings(params));
    }

    @Test
    void celestaRunsInMixedModeOnTcpPort() throws CelestaException, SQLException {
        Properties params = new Properties();
        params.setProperty("score.path", "score");
        params.setProperty("h2.in-memory", "true");
        params.setProperty("h2.port", port);
        try (Celesta ignored = Celesta.createInstance(params);
             Connection conn = DriverManager.getConnection(jdbcURL)
        ) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select count(*) from \"celesta\".\"grains\"");
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) > 0);
        }
    }
}

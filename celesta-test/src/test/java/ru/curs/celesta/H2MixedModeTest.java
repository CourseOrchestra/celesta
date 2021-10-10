package ru.curs.celesta;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class H2MixedModeTest {

    private final String port = Integer.toString(6557 + ThreadLocalRandom.current().nextInt(20));
    private final String jdbcURL = String.format(
            "jdbc:h2:tcp://localhost:%s/mem:celesta", port);

    @Test
    void h2InMemorySetupPropertyIsParsed() {
        Properties params = new Properties();
        params.setProperty("score.path", "score");
        params.setProperty("h2.in-memory", "true");
        BaseAppSettings as = new AppSettings(params);
        //embedded mode
        assertEquals(BaseAppSettings.H2_IN_MEMORY_URL, as.getDatabaseConnection());

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
    void celestaRunsInMixedModeOnTcpPort() throws SQLException {
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

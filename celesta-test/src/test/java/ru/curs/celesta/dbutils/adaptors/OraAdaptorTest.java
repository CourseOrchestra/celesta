package ru.curs.celesta.dbutils.adaptors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.OracleContainer;
import ru.curs.celesta.AppSettings;
import ru.curs.celesta.BaseAppSettings;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.ConnectionPoolConfiguration;
import ru.curs.celesta.InternalConnectionPool;
import ru.curs.celesta.dbutils.DbUpdaterImpl;
import ru.curs.celesta.dbutils.adaptors.ddl.JdbcDdlConsumer;
import ru.curs.celesta.score.AbstractScore;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.discovery.ScoreByScorePathDiscovery;
import ru.curs.celesta.test.ContainerUtils;

import java.sql.Connection;
import java.util.Locale;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OraAdaptorTest extends AbstractAdaptorTest {

    static {
        Locale.setDefault(Locale.US);
    }

    public static OracleContainer oracle;

    private static OraAdaptor dba;

    @BeforeAll
    public static void beforeAll() throws Exception {
        oracle = ContainerUtils.getOracleContainer();
        oracle.start();

        Properties params = new Properties();
        params.put("score.path", "score");
        params.put("rdbms.connection.url", oracle.getJdbcUrl().replace("localhost", "0.0.0.0"));
        params.put("rdbms.connection.username", oracle.getUsername());
        params.put("rdbms.connection.password", oracle.getPassword());

        BaseAppSettings appSettings = new AppSettings(params);
        ConnectionPoolConfiguration cpc = new ConnectionPoolConfiguration();
        cpc.setJdbcConnectionUrl(appSettings.getDatabaseConnection());
        cpc.setDriverClassName(appSettings.getDbClassName());
        cpc.setLogin(appSettings.getDBLogin());
        cpc.setPassword(appSettings.getDBPassword());
        ConnectionPool connectionPool = InternalConnectionPool.create(cpc);

        dba = new OraAdaptor(connectionPool, new JdbcDdlConsumer());

        Score score = new AbstractScore.ScoreBuilder<>(Score.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(SCORE_NAME))
                .build();

        DbUpdaterImpl dbUpdater = createDbUpdater(score, dba);
        dbUpdater.updateSysGrain();
    }

    @AfterAll
    public static void afterAll() {
        dba.connectionPool.close();
        oracle.stop();
    }

    public OraAdaptorTest() throws Exception {
        setDba(dba);
        setScore(
                new AbstractScore.ScoreBuilder<>(Score.class)
                        .scoreDiscovery(new ScoreByScorePathDiscovery(SCORE_NAME))
                        .build()
        );
    }

    @Override
    Connection getConnection() {
        return dba.connectionPool.get();
    }

    @Override
    public void pkConstraintString() {
        final String pkName = dba.pkConstraintString(this.t);
        assertEquals("pk_test_gtest", pkName);
    }

}

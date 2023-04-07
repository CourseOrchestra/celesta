package ru.curs.celesta.dbutils.adaptors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;
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
import java.util.Properties;

public class PostgresAdaptorTest extends AbstractAdaptorTest {

    public static PostgreSQLContainer<?> postgres;

    private static PostgresAdaptor dba;

    @BeforeAll
    public static void beforeAll() throws Exception {
        postgres = ContainerUtils.getPostgreSQLContainer();
        postgres.start();

        Properties params = new Properties();
        params.put("score.path", "score");
        params.put("rdbms.connection.url", postgres.getJdbcUrl());
        params.put("rdbms.connection.username", postgres.getUsername());
        params.put("rdbms.connection.password", postgres.getPassword());

        BaseAppSettings appSettings = new AppSettings(params);
        ConnectionPoolConfiguration cpc = new ConnectionPoolConfiguration();
        cpc.setJdbcConnectionUrl(appSettings.getDatabaseConnection());
        cpc.setDriverClassName(appSettings.getDbClassName());
        cpc.setLogin(appSettings.getDBLogin());
        cpc.setPassword(appSettings.getDBPassword());
        ConnectionPool connectionPool = InternalConnectionPool.create(cpc);

        dba = new PostgresAdaptor(connectionPool, new JdbcDdlConsumer());

        Score score = new AbstractScore.ScoreBuilder<>(Score.class)
                .scoreDiscovery(new ScoreByScorePathDiscovery(SCORE_NAME))
                .build();

        DbUpdaterImpl dbUpdater = createDbUpdater(score, dba);
        dbUpdater.updateSysGrain();
    }

    @AfterAll
    public static void afterAll() {
        dba.connectionPool.close();
        postgres.stop();
    }

    public PostgresAdaptorTest() throws Exception {
        setDba(dba);
        setScore(
                new AbstractScore.ScoreBuilder<>(Score.class)
                        .scoreDiscovery(new ScoreByScorePathDiscovery(SCORE_NAME))
                        .build());
    }

    @Override
    Connection getConnection() {
        return dba.connectionPool.get();
    }

}

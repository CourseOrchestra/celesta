package ru.curs.celesta.dbutils.adaptors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import ru.curs.celesta.*;
import ru.curs.celesta.dbutils.*;
import ru.curs.celesta.dbutils.adaptors.ddl.JdbcDdlConsumer;
import ru.curs.celesta.score.AbstractScore;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.discovery.DefaultScoreDiscovery;


import java.sql.Connection;
import java.util.Properties;

/**
 * Created by ioann on 03.05.2017.
 */
public class H2AdaptorTest extends AbstractAdaptorTest {

    private static H2Adaptor dba;

    @BeforeAll
    public static void beforeAll() throws Exception {
        Properties params = new Properties();
        params.put("score.path", "score");
        params.put("h2.in-memory", "true");
        params.put("h2.referential.integrity", "true");

        BaseAppSettings appSettings = new AppSettings(params);

        ConnectionPoolConfiguration cpc = new ConnectionPoolConfiguration();
        cpc.setJdbcConnectionUrl(appSettings.getDatabaseConnection());
        cpc.setDriverClassName(appSettings.getDbClassName());
        cpc.setLogin(appSettings.getDBLogin());
        cpc.setPassword(appSettings.getDBPassword());

        ConnectionPool connectionPool = ConnectionPool.create(cpc);

        dba = new H2Adaptor(connectionPool, new JdbcDdlConsumer(), appSettings.isH2ReferentialIntegrity());

        Score score = new AbstractScore.ScoreBuilder<>(Score.class)
                .path(SCORE_NAME)
                .scoreDiscovery(new DefaultScoreDiscovery())
                .build();

        DbUpdaterImpl dbUpdater = createDbUpdater(score, dba);
        dbUpdater.updateSysGrain();
    }

    @AfterAll
    static void afterAll() throws Exception {
        dba.connectionPool.get().createStatement().execute("SHUTDOWN");
        dba.connectionPool.close();
    }

    public H2AdaptorTest() throws Exception {
        setDba(dba);
        setScore(
                new AbstractScore.ScoreBuilder<>(Score.class)
                        .path(SCORE_NAME)
                        .scoreDiscovery(new DefaultScoreDiscovery())
                        .build()
        );
    }

    @Override
    Connection getConnection() {
        return dba.connectionPool.get();
    }
}

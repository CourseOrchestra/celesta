package ru.curs.celesta.dbutils.adaptors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import ru.curs.celesta.*;
import ru.curs.celesta.dbutils.DbUpdaterImpl;
import ru.curs.celesta.dbutils.DbUpdaterBuilder;
import ru.curs.celesta.dbutils.LoggingManager;
import ru.curs.celesta.dbutils.PermissionManager;
import ru.curs.celesta.score.AbstractScore;
import ru.curs.celesta.score.discovery.PyScoreDiscovery;


import java.sql.Connection;
import java.util.Properties;

/**
 * Created by ioann on 03.05.2017.
 */
public class H2AdaptorTest extends AbstractAdaptorTest {

    private static H2Adaptor dba;

    @BeforeAll
    public static void beforeAll() throws CelestaException {
        Properties params = new Properties();
        params.put("score.path", "score");
        params.put("h2.in-memory", "true");
        params.put("h2.referential.integrity", "true");

        AppSettings appSettings = new AppSettings(params);

        ConnectionPoolConfiguration cpc = new ConnectionPoolConfiguration();
        cpc.setJdbcConnectionUrl(appSettings.getDatabaseConnection());
        cpc.setDriverClassName(appSettings.getDbClassName());
        cpc.setLogin(appSettings.getDBLogin());
        cpc.setPassword(appSettings.getDBPassword());

        ConnectionPool connectionPool = ConnectionPool.create(cpc);

        dba = new H2Adaptor(connectionPool, appSettings.isH2ReferentialIntegrity());

        DbUpdaterImpl dbUpdater = new DbUpdaterBuilder()
                .dbAdaptor(dba)
                .connectionPool(connectionPool)
                .score(
                        new AbstractScore.ScoreBuilder()
                                .path(SCORE_NAME)
                                .scoreDiscovery(new PyScoreDiscovery())
                                .build()
                )
                .setPermissionManager(new PermissionManager(dba))
                .setLoggingManager(new LoggingManager(dba))
                .build();

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
                new AbstractScore.ScoreBuilder()
                .path(SCORE_NAME)
                .scoreDiscovery(new PyScoreDiscovery())
                .build()
        );
    }

    @Override
    Connection getConnection() throws CelestaException {
        return dba.connectionPool.get();
    }

}

package ru.curs.celesta.dbutils.adaptors;

import java.sql.Connection;
import java.util.Properties;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.MSSQLServerContainer;
import ru.curs.celesta.*;
import ru.curs.celesta.dbutils.DbUpdaterImpl;
import ru.curs.celesta.dbutils.DbUpdaterBuilder;
import ru.curs.celesta.dbutils.LoggingManager;
import ru.curs.celesta.dbutils.PermissionManager;
import ru.curs.celesta.score.AbstractScore;
import ru.curs.celesta.score.discovery.PyScoreDiscovery;

public class MSSQLAdaptorTest extends AbstractAdaptorTest {

    public static MSSQLServerContainer msSql = new MSSQLServerContainer()
            .withDatabaseName("celesta")
            .withCollation("Cyrillic_General_CI_AI");

    private static MSSQLAdaptor dba;

    @BeforeAll
    public static void beforeAll() throws Exception {
        msSql.start();

        Properties params = new Properties();
        params.put("score.path", "score");
        params.put("rdbms.connection.url", msSql.getJdbcUrl());
        params.put("rdbms.connection.username", msSql.getUsername());
        params.put("rdbms.connection.password", msSql.getPassword());

        AppSettings appSettings = new AppSettings(params);
        ConnectionPoolConfiguration cpc = new ConnectionPoolConfiguration();
        cpc.setJdbcConnectionUrl(appSettings.getDatabaseConnection());
        cpc.setDriverClassName(appSettings.getDbClassName());
        cpc.setLogin(appSettings.getDBLogin());
        cpc.setPassword(appSettings.getDBPassword());
        ConnectionPool connectionPool = ConnectionPool.create(cpc);

        dba = new MSSQLAdaptor(connectionPool);

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
    public static void destroy() {
        msSql.stop();
    }

    public MSSQLAdaptorTest() throws Exception {
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

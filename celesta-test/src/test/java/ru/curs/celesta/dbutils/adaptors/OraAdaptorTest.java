package ru.curs.celesta.dbutils.adaptors;

import java.sql.Connection;
import java.util.Locale;
import java.util.Properties;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.OracleContainer;
import ru.curs.celesta.*;
import ru.curs.celesta.dbutils.DbUpdaterImpl;
import ru.curs.celesta.dbutils.DbUpdaterBuilder;
import ru.curs.celesta.dbutils.LoggingManager;
import ru.curs.celesta.dbutils.PermissionManager;
import ru.curs.celesta.dbutils.adaptors.ddl.JdbcDdlConsumer;
import ru.curs.celesta.mock.CelestaImpl;
import ru.curs.celesta.score.AbstractScore;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.discovery.PyScoreDiscovery;

public class OraAdaptorTest extends AbstractAdaptorTest {

    static {
        Locale.setDefault(Locale.US);
    }

    public static OracleContainer oracle = new OracleContainer();

    private static OraAdaptor dba;

    @BeforeAll
    public static void beforeAll() throws Exception {
        oracle.start();

        Properties params = new Properties();
        params.put("score.path", "score");
        params.put("rdbms.connection.url", oracle.getJdbcUrl().replace("localhost", "0.0.0.0"));
        params.put("rdbms.connection.username", oracle.getUsername());
        params.put("rdbms.connection.password", oracle.getPassword());

        AppSettings appSettings = new AppSettings(params);
        ConnectionPoolConfiguration cpc = new ConnectionPoolConfiguration();
        cpc.setJdbcConnectionUrl(appSettings.getDatabaseConnection());
        cpc.setDriverClassName(appSettings.getDbClassName());
        cpc.setLogin(appSettings.getDBLogin());
        cpc.setPassword(appSettings.getDBPassword());
        ConnectionPool connectionPool = ConnectionPool.create(cpc);

        dba = new OraAdaptor(connectionPool, new JdbcDdlConsumer());

        Score score = new AbstractScore.ScoreBuilder<>(Score.class)
                .path(SCORE_NAME)
                .scoreDiscovery(new PyScoreDiscovery())
                .build();
        CelestaImpl celesta = new CelestaImpl(dba, connectionPool, score);
        PermissionManager permissionManager = celesta.getPermissionManager();
        LoggingManager loggingManager = celesta.getLoggingManager();

        DbUpdaterImpl dbUpdater = new DbUpdaterBuilder()
                .dbAdaptor(dba)
                .connectionPool(connectionPool)
                .score(score)
                .setCelesta(celesta)
                .setPermissionManager(permissionManager)
                .setLoggingManager(loggingManager)
                .build();

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
                        .path(SCORE_NAME)
                        .scoreDiscovery(new PyScoreDiscovery())
                        .build()
        );
    }

    @Override
    Connection getConnection() {
        return dba.connectionPool.get();
    }

}

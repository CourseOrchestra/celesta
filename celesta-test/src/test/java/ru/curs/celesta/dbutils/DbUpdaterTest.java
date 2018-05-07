package ru.curs.celesta.dbutils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.curs.celesta.*;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.dbutils.adaptors.H2Adaptor;
import ru.curs.celesta.dbutils.adaptors.ddl.JdbcDdlConsumer;
import ru.curs.celesta.mock.CelestaImpl;
import ru.curs.celesta.score.AbstractScore;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.discovery.PyScoreDiscovery;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Properties;

public class DbUpdaterTest {


    private ConnectionPool connectionPool;


    @AfterEach
    void after() throws Exception {
        this.connectionPool.get().createStatement().execute("SHUTDOWN");
        this.connectionPool.close();
    }


    @Test
    void testFailWithExecNativeBefore() throws Exception {
        DbUpdater dbUpdater = createDbUpdater("db_updater_test/nativeBeforeExceptionScore");
        assertThrows(
                CelestaException.class,
                () -> dbUpdater.updateDb(),
                DbUpdaterImpl.EXEC_NATIVE_NOT_SUPPORTED_MESSAGE
        );
    }

    @Test
    void testFailWithExecNativeAfter() throws Exception {
        DbUpdater dbUpdater = createDbUpdater("db_updater_test/nativeAfterExceptionScore");
        assertThrows(
                CelestaException.class,
                () -> dbUpdater.updateDb(),
                DbUpdaterImpl.EXEC_NATIVE_NOT_SUPPORTED_MESSAGE
        );
    }


    private DbUpdater createDbUpdater(String scoreResourcePath) throws Exception {
        String scorePath = getClass().getResource(scoreResourcePath).getPath();

        Properties params = new Properties();
        params.put("score.path", scorePath);
        params.put("h2.in-memory", "true");
        params.put("h2.referential.integrity", "true");

        BaseAppSettings appSettings = new JythonAppSettings(params);

        ConnectionPoolConfiguration cpc = new ConnectionPoolConfiguration();
        cpc.setJdbcConnectionUrl(appSettings.getDatabaseConnection());
        cpc.setDriverClassName(appSettings.getDbClassName());
        cpc.setLogin(appSettings.getDBLogin());
        cpc.setPassword(appSettings.getDBPassword());

        this.connectionPool = ConnectionPool.create(cpc);

        DBAdaptor dba = new H2Adaptor(this.connectionPool, new JdbcDdlConsumer(), appSettings.isH2ReferentialIntegrity());

        Score score = new AbstractScore.ScoreBuilder<>(Score.class)
                .path(scorePath)
                .scoreDiscovery(new PyScoreDiscovery())
                .build();

        CelestaImpl celesta = new CelestaImpl(dba, connectionPool, score);
        PermissionManager permissionManager = celesta.getPermissionManager();
        LoggingManager loggingManager = celesta.getLoggingManager();

        DbUpdaterImpl dbUpdater = new DbUpdaterBuilder()
                .dbAdaptor(dba)
                .connectionPool(this.connectionPool)
                .score(score)
                .setCelesta(celesta)
                .setPermissionManager(permissionManager)
                .setLoggingManager(loggingManager)
                .build();

        return dbUpdater;
    }

}

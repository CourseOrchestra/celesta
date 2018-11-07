package ru.curs.celesta;

import ru.curs.celesta.dbutils.*;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.dbutils.adaptors.configuration.DbAdaptorFactory;
import ru.curs.celesta.dbutils.adaptors.ddl.JdbcDdlConsumer;
import ru.curs.celesta.test.mock.CelestaImpl;
import ru.curs.celesta.score.AbstractScore;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.discovery.DefaultScoreDiscovery;

import java.util.Properties;

public class TestUtils {


    public static DbUpdater createDbUpdater(DBType type, String scorePath) {
        Properties params = new Properties();
        params.put("score.path", scorePath);
        params.put("h2.in-memory", "true");
        params.put("h2.referential.integrity", "true");

        BaseAppSettings appSettings = new AppSettings(params);

        ConnectionPoolConfiguration cpc = new ConnectionPoolConfiguration();
        cpc.setJdbcConnectionUrl(appSettings.getDatabaseConnection());
        cpc.setDriverClassName(appSettings.getDbClassName());
        cpc.setLogin(appSettings.getDBLogin());
        cpc.setPassword(appSettings.getDBPassword());

        ConnectionPool connectionPool = ConnectionPool.create(cpc);

        DBAdaptor dba = createDbAdaptor(type, connectionPool);

        final Score score;

        try {
            score = new AbstractScore.ScoreBuilder<>(Score.class)
                    .path(scorePath)
                    .scoreDiscovery(new DefaultScoreDiscovery())
                    .build();
        } catch (ParseException e) {
            throw new CelestaException(e);
        }

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

        return dbUpdater;
    }


    private static DBAdaptor createDbAdaptor(DBType dbType, ConnectionPool connectionPool) {
        return new DbAdaptorFactory()
                .setDbType(dbType)
                .setDdlConsumer(new JdbcDdlConsumer())
                .setConnectionPool(connectionPool)
                .setH2ReferentialIntegrity(false)
                .createDbAdaptor();
    }
}

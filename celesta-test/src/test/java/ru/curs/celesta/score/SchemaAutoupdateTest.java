package ru.curs.celesta.score;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import ru.curs.celesta.*;
import ru.curs.celesta.dbutils.DbUpdater;
import ru.curs.celesta.dbutils.DbUpdaterBuilder;
import ru.curs.celesta.dbutils.DbUpdaterImpl;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.dbutils.adaptors.H2Adaptor;
import ru.curs.celesta.dbutils.adaptors.ddl.JdbcDdlConsumer;
import ru.curs.celesta.mock.CelestaImpl;
import ru.curs.celesta.score.AbstractScore;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.discovery.DefaultScoreDiscovery;
import ru.curs.celesta.syscursors.GrainsCursor;
import ru.curs.celesta.syscursors.ISchemaCursor;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.util.Properties;

/**
 * @author Pavel Perminov (packpaul@mail.ru)
 * @since 2018-10-12
 */
public class SchemaAutoupdateTest {

    private static CelestaImpl celesta;

    @AfterAll
    static void afterAll() throws Exception {
        if (celesta != null) {
            celesta.getConnectionPool().get().createStatement().execute("SHUTDOWN");
            celesta.close();
        }
    }

    @Test
    void testScoreParsing() throws Exception {
        celesta = getCelesta("schema_autoupdate/scoreV2");
        assertTrue(celesta.getScore().getGrain("A").isAutoupdate());
        assertFalse(celesta.getScore().getGrain("B").isAutoupdate());
        assertTrue(celesta.getScore().getGrain("C").isAutoupdate());
    }
    
    @Test
    void testWithNoAutoupdateOption() throws Exception {

        CelestaImpl celesta = getCelesta("schema_autoupdate/scoreV1");
        DbUpdater<?> dbUpdater = createDbUpdater(celesta);
        dbUpdater.updateDb();

        Connection conn = celesta.getConnectionPool().get();
        DBAdaptor dba = celesta.getDBAdaptor();

        assertTrue(dba.tableExists(conn, "A", "a"));
        assertTrue(dba.tableExists(conn, "B", "b"));
        assertTrue(dba.tableExists(conn, "C", "c"));
        
        lockGrain("A", celesta);

        celesta = getCelesta("schema_autoupdate/scoreV2");
        dbUpdater = createDbUpdater(celesta);
        dbUpdater.updateDb();

        conn = celesta.getConnectionPool().get();
        dba = celesta.getDBAdaptor();

        Table tableAa = celesta.getScore().getGrain("A").getTable("a");
        assertFalse(dba.getColumns(conn, tableAa).contains("title"));

        Table tableBb = celesta.getScore().getGrain("B").getTable("b");
        assertFalse(dba.getColumns(conn, tableBb).contains("title"));

        Table tableCc = celesta.getScore().getGrain("C").getTable("c");
        assertTrue(dba.getColumns(conn, tableCc).contains("title"));       
    }
    
    private void lockGrain(String grainName, ICelesta celesta) {
        try(CallContext cc = new SystemCallContext(celesta)) {
            GrainsCursor gc = new GrainsCursor(cc); 
            gc.get(grainName);
            gc.setState(ISchemaCursor.LOCK);
            gc.update();
            gc.close();
        }
    }

    private static DbUpdater<?> createDbUpdater(CelestaImpl celesta) {

        DbUpdaterImpl dbUpdater = new DbUpdaterBuilder()
                .dbAdaptor(celesta.getDBAdaptor())
                .connectionPool(celesta.getConnectionPool())
                .score(celesta.getScore())
                .setCelesta(celesta)
                .setPermissionManager(celesta.getPermissionManager())
                .setLoggingManager(celesta.getLoggingManager())
                .build();
        
        return dbUpdater;
    }

    private static CelestaImpl getCelesta(String scoreResourcePath) throws Exception {

        String scorePath = SchemaAutoupdateTest.class.getResource(scoreResourcePath).getPath();

        if ((celesta != null) && (! celesta.isClosed()) && (scorePath.equals(celesta.getScore().getPath()))) {
            return celesta;
        }

        ConnectionPool connectionPool;
        DBAdaptor dba;

        if ((celesta != null) && (! celesta.isClosed())) {
            connectionPool = celesta.getConnectionPool();
            dba = celesta.getDBAdaptor();
        } else {
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

            connectionPool = ConnectionPool.create(cpc);
            dba = new H2Adaptor(connectionPool, new JdbcDdlConsumer(), appSettings.isH2ReferentialIntegrity());
        }

        Score score = new AbstractScore.ScoreBuilder<>(Score.class)
                .path(scorePath)
                .scoreDiscovery(new DefaultScoreDiscovery())
                .build();

        return (celesta = new CelestaImpl(dba, connectionPool, score));
    }

}

package ru.curs.celesta.dbutils.adaptors;

import org.firebirdsql.testcontainers.FirebirdContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.curs.celesta.AppSettings;
import ru.curs.celesta.BaseAppSettings;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.ConnectionPoolConfiguration;
import ru.curs.celesta.dbutils.DbUpdaterImpl;
import ru.curs.celesta.dbutils.adaptors.ddl.JdbcDdlConsumer;
import ru.curs.celesta.score.AbstractScore;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.discovery.ScoreByScorePathDiscovery;
import ru.curs.celesta.test.ContainerUtils;

import java.sql.Connection;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FirebirdAdaptorTest extends AbstractAdaptorTest {

    private static String SCORE_NAME = "testScore_firebird";

    private static FirebirdContainer firebird = ContainerUtils.FIREBIRD;

    private static FirebirdAdaptor dba;

    @BeforeAll
    public static void beforeAll() throws Exception {
        firebird.start();

        Properties params = new Properties();
        params.put("score.path", "score");
        params.put("rdbms.connection.url", firebird.getJdbcUrl());
        params.put("rdbms.connection.username", firebird.getUsername());
        params.put("rdbms.connection.password", firebird.getPassword());

        BaseAppSettings appSettings = new AppSettings(params);
        ConnectionPoolConfiguration cpc = new ConnectionPoolConfiguration();
        // TODO:: DISCUSS DEFAULT ENCODING
        cpc.setJdbcConnectionUrl(appSettings.getDatabaseConnection() + "?encoding=UNICODE_FSS");
        cpc.setDriverClassName(appSettings.getDbClassName());
        cpc.setLogin(appSettings.getDBLogin());
        cpc.setPassword(appSettings.getDBPassword());
        ConnectionPool connectionPool = ConnectionPool.create(cpc);

        dba = new FirebirdAdaptor(connectionPool, new JdbcDdlConsumer());

        Score score = new AbstractScore.ScoreBuilder<>(Score.class)
            .scoreDiscovery(new ScoreByScorePathDiscovery(SCORE_NAME))
            .build();

        DbUpdaterImpl dbUpdater = createDbUpdater(score, dba);
        dbUpdater.updateSysGrain();
    }

    @AfterAll
    public static void destroy() {
        dba.connectionPool.close();
        ContainerUtils.cleanUp(firebird);
    }

    public FirebirdAdaptorTest() throws Exception {
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

    @Test
    @Override
    public void pkConstraintString() {
        final String pkName = dba.pkConstraintString(this.t);
        assertEquals("pk_test_gtest", pkName);
    }
}

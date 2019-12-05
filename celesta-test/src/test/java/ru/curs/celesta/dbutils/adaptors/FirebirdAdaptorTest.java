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
import ru.curs.celesta.dbutils.meta.DbColumnInfo;
import ru.curs.celesta.score.AbstractScore;
import ru.curs.celesta.score.BinaryColumn;
import ru.curs.celesta.score.BooleanColumn;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.DecimalColumn;
import ru.curs.celesta.score.FloatingColumn;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.StringColumn;
import ru.curs.celesta.score.ZonedDateTimeColumn;
import ru.curs.celesta.score.discovery.ScoreByScorePathDiscovery;
import ru.curs.celesta.test.ContainerUtils;

import java.sql.Connection;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

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

    @Test
    @Override
    public void getColumnInfo1() throws ParseException {
        DbColumnInfo c;
        // Проверяем реакцию на столбец, которого нет в базе данных
        Column<?> newCol = new IntegerColumn(t, "nonExistentColumn");
        assertSame(newCol, t.getColumn("nonExistentColumn"));
        c = dba.getColumnInfo(conn, newCol);
        assertNull(c);

        // Этот тест проверяет типы колонок и выражения not null
        // attrVarchar varchar(2),
        c = dba.getColumnInfo(conn, t.getColumn("attrVarchar"));
        assertEquals("attrVarchar", c.getName());
        assertSame(StringColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals(2, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("", c.getDefaultValue());
        assertEquals(false, c.isMax());

        // f1 bit not null,
        c = dba.getColumnInfo(conn, t.getColumn("f1"));
        assertEquals("f1", c.getName());
        assertSame(BooleanColumn.class, c.getType());
        assertEquals(false, c.isNullable());
        assertEquals(0, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("", c.getDefaultValue());
        assertEquals(false, c.isMax());

        // f4 real,
        c = dba.getColumnInfo(conn, t.getColumn("f4"));
        assertEquals("f4", c.getName());
        assertSame(FloatingColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals(0, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("", c.getDefaultValue());
        assertEquals(false, c.isMax());

        // f7 varchar(8),
        c = dba.getColumnInfo(conn, t.getColumn("f7"));
        assertEquals("f7", c.getName());
        assertSame(StringColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals(8, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("", c.getDefaultValue());
        assertEquals(false, c.isMax());

        // f11 image not null
        c = dba.getColumnInfo(conn, t.getColumn("f11"));
        assertEquals("f11", c.getName());
        assertSame(BinaryColumn.class, c.getType());
        assertEquals(false, c.isNullable());
        assertEquals(0, c.getLength());
        assertEquals(0, c.getScale());
        assertEquals("", c.getDefaultValue());
        assertEquals(false, c.isMax());

        // f12 decimal(11, 7),
        c = dba.getColumnInfo(conn, t.getColumn("f12"));
        assertEquals("f12", c.getName());
        assertSame(DecimalColumn.class, c.getType());
        assertEquals(true, c.isNullable());
        assertEquals(11, c.getLength());
        assertEquals(7, c.getScale());
        assertEquals("", c.getDefaultValue());
        assertEquals(false, c.isMax());
    }

}

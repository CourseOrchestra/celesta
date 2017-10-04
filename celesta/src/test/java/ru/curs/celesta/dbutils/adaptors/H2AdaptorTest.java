package ru.curs.celesta.dbutils.adaptors;

import ru.curs.celesta.*;
import ru.curs.celesta.score.Score;

import java.io.InputStream;

import java.sql.Connection;
import java.util.Properties;

/**
 * Created by ioann on 03.05.2017.
 */
public class H2AdaptorTest extends AbstractAdaptorTest {

  private final ConnectionPool connectionPool;

  public H2AdaptorTest() throws Exception {
    Properties params = new Properties();
    InputStream is = InitTest.class
        .getResourceAsStream("celesta.h2.properties");
    params.load(is);

    AppSettings appSettings = new AppSettings(params);

    ConnectionPoolConfiguration cpc = new ConnectionPoolConfiguration();
    cpc.setJdbcConnectionUrl(appSettings.getDatabaseConnection());
    cpc.setDriverClassName(appSettings.getDbClassName());
    cpc.setLogin(appSettings.getDBLogin());
    cpc.setPassword(appSettings.getDBPassword());

    connectionPool = ConnectionPool.create(cpc);

    DBAdaptor dba = new H2Adaptor(connectionPool, false);

    setDba(dba);
    setScore(new Score(SCORE_NAME));
  }

  @Override
  Connection getConnection() throws CelestaException {
    return connectionPool.get();
  }

}

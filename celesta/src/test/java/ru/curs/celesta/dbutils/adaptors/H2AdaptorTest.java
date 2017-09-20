package ru.curs.celesta.dbutils.adaptors;

import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import ru.curs.celesta.*;
import ru.curs.celesta.score.Score;

import java.io.InputStream;

import java.util.Properties;

/**
 * Created by ioann on 03.05.2017.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest( DBAdaptor.class )
@PowerMockIgnore({
    "javax.management.*", //https://github.com/powermock/powermock/issues/743#issuecomment-287843821
})
public class H2AdaptorTest extends AbstractAdaptorTest {


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

    ConnectionPool.init(cpc);

    DBAdaptor dba = new H2Adaptor();
    initMocks(dba);

    ConnectionPool.clear();

    setDba(dba);
    setScore(new Score(SCORE_NAME));
  }


  public void initMocks(DBAdaptor dba) throws CelestaException {
    PowerMockito.stub(
        PowerMockito.method(
            DBAdaptor.class, "getAdaptor"
        )
    ).toReturn(dba);
  }
}

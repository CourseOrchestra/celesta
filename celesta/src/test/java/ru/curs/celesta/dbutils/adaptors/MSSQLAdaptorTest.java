package ru.curs.celesta.dbutils.adaptors;

import java.util.Properties;

import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import ru.curs.celesta.*;
import ru.curs.celesta.score.Score;

@RunWith(PowerMockRunner.class)
@PrepareForTest( DBAdaptor.class )
@PowerMockIgnore({
		"java.security.*",   //https://github.com/powermock/powermock/issues/308
		"javax.management.*" //https://github.com/powermock/powermock/issues/743#issuecomment-287843821
})
public class MSSQLAdaptorTest extends AbstractAdaptorTest {

	public MSSQLAdaptorTest() throws Exception {
		Properties params = new Properties();
		params.load(InitTest.class
				.getResourceAsStream("celesta.mssql.properties"));

		AppSettings appSettings = new AppSettings(params);

		ConnectionPoolConfiguration cpc = new ConnectionPoolConfiguration();
		cpc.setJdbcConnectionUrl(appSettings.getDatabaseConnection());
		cpc.setDriverClassName(appSettings.getDbClassName());
		cpc.setLogin(appSettings.getDBLogin());
		cpc.setPassword(appSettings.getDBPassword());

		ConnectionPool.init(cpc);

		DBAdaptor dba = new MSSQLAdaptor();
		initMocks(dba);

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

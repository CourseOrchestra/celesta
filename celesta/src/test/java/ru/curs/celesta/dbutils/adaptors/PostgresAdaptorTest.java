package ru.curs.celesta.dbutils.adaptors;

import java.sql.Connection;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.testcontainers.containers.PostgreSQLContainer;
import ru.curs.celesta.*;
import ru.curs.celesta.dbutils.DbUpdater;
import ru.curs.celesta.dbutils.DbUpdaterBuilder;
import ru.curs.celesta.score.Score;

@RunWith(PowerMockRunner.class)
@PrepareForTest( DBAdaptor.class )
@PowerMockIgnore({
		"javax.management.*", //https://github.com/powermock/powermock/issues/743#issuecomment-287843821
})
public class PostgresAdaptorTest extends AbstractAdaptorTest {

	@ClassRule
	static PostgreSQLContainer postgres = new PostgreSQLContainer();

	private static PostgresAdaptor dba;

	@BeforeClass
	public static void beforeAll() throws CelestaException {
		postgres.start();

		Properties params = new Properties();
		params.put("score.path", "score");
		params.put("rdbms.connection.url", postgres.getJdbcUrl());
		params.put("rdbms.connection.username", postgres.getUsername());
		params.put("rdbms.connection.password", postgres.getPassword());

		AppSettings appSettings = new AppSettings(params);
		ConnectionPoolConfiguration cpc = new ConnectionPoolConfiguration();
		cpc.setJdbcConnectionUrl(appSettings.getDatabaseConnection());
		cpc.setDriverClassName(appSettings.getDbClassName());
		cpc.setLogin(appSettings.getDBLogin());
		cpc.setPassword(appSettings.getDBPassword());
		ConnectionPool connectionPool = ConnectionPool.create(cpc);

		dba = new PostgresAdaptor(connectionPool);
		initMocks(dba);

		DbUpdater dbUpdater = new DbUpdaterBuilder()
				.dbAdaptor(dba)
				.connectionPool(connectionPool)
				.score(new Score(SCORE_NAME))
				.build();

		dbUpdater.updateSysGrain();
	}

	public PostgresAdaptorTest() throws Exception {
		setDba(dba);
		initMocks(dba);
		setScore(new Score(SCORE_NAME));
	}

	@Override
	Connection getConnection() throws CelestaException {
		return dba.connectionPool.get();
	}

	public static void initMocks(DBAdaptor dba) throws CelestaException {
		PowerMockito.stub(
				PowerMockito.method(
						DBAdaptor.class, "getAdaptor"
				)
		).toReturn(dba);
	}
}

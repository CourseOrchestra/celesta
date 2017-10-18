package ru.curs.celesta.dbutils.adaptors;

import java.sql.Connection;
import java.util.Locale;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.testcontainers.containers.OracleContainer;
import ru.curs.celesta.*;
import ru.curs.celesta.dbutils.DbUpdater;
import ru.curs.celesta.dbutils.DbUpdaterBuilder;
import ru.curs.celesta.dbutils.LoggingManager;
import ru.curs.celesta.dbutils.PermissionManager;
import ru.curs.celesta.score.Score;

public class OraAdaptorTest extends AbstractAdaptorTest {

	static {
		Locale.setDefault(Locale.US);
	}

	@ClassRule
	public static OracleContainer oracle = new OracleContainer();

	private static OraAdaptor dba;

	@BeforeClass
	public static void beforeAll() throws CelestaException {

		Properties params = new Properties();
		params.put("score.path", "score");
		params.put("rdbms.connection.url", oracle.getJdbcUrl());
		params.put("rdbms.connection.username", oracle.getUsername());
		params.put("rdbms.connection.password", oracle.getPassword());

		AppSettings appSettings = new AppSettings(params);
		ConnectionPoolConfiguration cpc = new ConnectionPoolConfiguration();
		cpc.setJdbcConnectionUrl(appSettings.getDatabaseConnection());
		cpc.setDriverClassName(appSettings.getDbClassName());
		cpc.setLogin(appSettings.getDBLogin());
		cpc.setPassword(appSettings.getDBPassword());
		ConnectionPool connectionPool = ConnectionPool.create(cpc);

		dba = new OraAdaptor(connectionPool);

		DbUpdater dbUpdater = new DbUpdaterBuilder()
				.dbAdaptor(dba)
				.connectionPool(connectionPool)
				.score(new Score(SCORE_NAME))
				.setPermissionManager(new PermissionManager(dba))
				.setLoggingManager(new LoggingManager(dba))
				.build();

		dbUpdater.updateSysGrain();
	}

	@AfterClass
	public static void afterAll() {
		dba.connectionPool.close();
	}

	public OraAdaptorTest() throws Exception {
		setDba(dba);
		setScore(new Score(SCORE_NAME));
	}

	@Override
	Connection getConnection() throws CelestaException {
		return dba.connectionPool.get();
	}

}

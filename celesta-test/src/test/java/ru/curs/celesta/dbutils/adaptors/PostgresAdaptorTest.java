package ru.curs.celesta.dbutils.adaptors;

import java.sql.Connection;
import java.util.Properties;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;
import ru.curs.celesta.*;
import ru.curs.celesta.dbutils.*;
import ru.curs.celesta.score.Score;

public class PostgresAdaptorTest extends AbstractAdaptorTest {

	public static PostgreSQLContainer postgres = new PostgreSQLContainer();

	private static PostgresAdaptor dba;

	@BeforeAll
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

		DbUpdater dbUpdater = new DbUpdaterBuilder()
				.dbAdaptor(dba)
				.connectionPool(connectionPool)
				.score(new Score(SCORE_NAME))
				.setPermissionManager(new PermissionManager(dba))
				.setLoggingManager(new LoggingManager(dba))
				.build();

		dbUpdater.updateSysGrain();
	}

	@AfterAll
	public static void afterAll() {
		dba.connectionPool.close();
		postgres.stop();
	}

	public PostgresAdaptorTest() throws Exception {
		setDba(dba);
		setScore(new Score(SCORE_NAME));
	}

	@Override
	Connection getConnection() throws CelestaException {
		return dba.connectionPool.get();
	}

}

package ru.curs.celesta.dbutils.adaptors;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.testcontainers.containers.MSSQLServerContainer;
import ru.curs.celesta.*;
import ru.curs.celesta.dbutils.DbUpdater;
import ru.curs.celesta.dbutils.DbUpdaterBuilder;
import ru.curs.celesta.dbutils.LoggingManager;
import ru.curs.celesta.dbutils.PermissionManager;
import ru.curs.celesta.score.Score;

public class MSSQLAdaptorTest extends AbstractAdaptorTest {

	@ClassRule
	public static MSSQLServerContainer msSql = new MSSQLServerContainer()
			.withDatabaseName("celesta")
			.withCollation("Cyrillic_General_CI_AI");

	private static MSSQLAdaptor dba;

	@BeforeClass
	public static void beforeAll() throws Exception {
		Properties params = new Properties();
		params.put("score.path", "score");
		params.put("rdbms.connection.url", msSql.getJdbcUrl());
		params.put("rdbms.connection.username", msSql.getUsername());
		params.put("rdbms.connection.password", msSql.getPassword());

		AppSettings appSettings = new AppSettings(params);
		ConnectionPoolConfiguration cpc = new ConnectionPoolConfiguration();
		cpc.setJdbcConnectionUrl(appSettings.getDatabaseConnection());
		cpc.setDriverClassName(appSettings.getDbClassName());
		cpc.setLogin(appSettings.getDBLogin());
		cpc.setPassword(appSettings.getDBPassword());
		ConnectionPool connectionPool = ConnectionPool.create(cpc);


		//Устанавливаем COLLATION
		try (Connection conn = connectionPool.get();
			 Statement stmt = conn.createStatement()
		) {
			//String sql = "ALTER DATABASE master COLLATE French_CI_AS";
			//stmt.executeUpdate(sql);
			//connectionPool.commit(conn);
		}

		dba = new MSSQLAdaptor(connectionPool);

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
	public static void destroy() {
		msSql.stop();
	}

	public MSSQLAdaptorTest() throws Exception {
		setDba(dba);
		setScore(new Score(SCORE_NAME));
	}

	@Override
	Connection getConnection() throws CelestaException {
		return dba.connectionPool.get();
	}

}

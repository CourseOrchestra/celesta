package ru.curs.celesta.dbutils.adaptors;

import java.sql.Connection;
import java.util.Properties;

import ru.curs.celesta.*;
import ru.curs.celesta.score.Score;

public class MSSQLAdaptorTest extends AbstractAdaptorTest {

	private final ConnectionPool connectionPool;

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

		connectionPool = ConnectionPool.create(cpc);

		DBAdaptor dba = new MSSQLAdaptor(connectionPool);

		setDba(dba);
		setScore(new Score(SCORE_NAME));
	}

	@Override
	Connection getConnection() throws CelestaException {
		return connectionPool.get();
	}


}

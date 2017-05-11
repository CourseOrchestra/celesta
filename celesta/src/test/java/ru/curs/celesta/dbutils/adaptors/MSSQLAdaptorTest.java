package ru.curs.celesta.dbutils.adaptors;

import java.lang.reflect.Method;
import java.util.Properties;

import ru.curs.celesta.AppSettings;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.InitTest;
import ru.curs.celesta.dbutils.adaptors.AbstractAdaptorTest;
import ru.curs.celesta.dbutils.adaptors.MSSQLAdaptor;
import ru.curs.celesta.score.Score;

public class MSSQLAdaptorTest extends AbstractAdaptorTest {

	public MSSQLAdaptorTest() throws Exception {
		ConnectionPool.clear();
		Properties params = new Properties();
		params.load(InitTest.class
				.getResourceAsStream("celesta.mssql.properties"));
		// Инициализация параметров приложения: вызов AppSettings.init(params) -
		// метод имеет модификатор доступа "по умолчанию"
		Method method = AppSettings.class.getDeclaredMethod("init",
				Properties.class);
		method.setAccessible(true);
		method.invoke(null, params);

		setDba(new MSSQLAdaptor());
		setScore(new Score(SCORE_NAME));
	}

}

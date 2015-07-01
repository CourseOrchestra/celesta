package ru.curs.celesta.dbutils;

import java.lang.reflect.Method;
import java.util.Properties;

import ru.curs.celesta.AppSettings;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.InitTest;
import ru.curs.celesta.score.Score;

public class OraAdaptorTest extends AbstractAdaptorTest {

	public OraAdaptorTest() throws Exception {
		ConnectionPool.clear();
		Properties params = new Properties();
		params.load(InitTest.class
				.getResourceAsStream("celesta.oracle.properties"));
		// Инициализация параметров приложения: вызов AppSettings.init(params) -
		// метод имеет модификатор доступа "по умолчанию"
		Method method = AppSettings.class.getDeclaredMethod("init",
				Properties.class);
		method.setAccessible(true);
		method.invoke(null, params);

		setDba(new OraAdaptor());
		setScore(new Score(SCORE_NAME));
	}

}

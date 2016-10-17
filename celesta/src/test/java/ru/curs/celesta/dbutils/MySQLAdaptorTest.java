package ru.curs.celesta.dbutils;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Properties;

import ru.curs.celesta.AppSettings;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.InitTest;
import ru.curs.celesta.score.Score;

public class MySQLAdaptorTest extends AbstractAdaptorTest {

	public MySQLAdaptorTest() throws Exception {
		ConnectionPool.clear();
		Properties params = new Properties();
		InputStream is = InitTest.class
				.getResourceAsStream("celesta.mysql.properties");
		params.load(is);
		// Инициализация параметров приложения: вызов AppSettings.init(params) -
		// метод имеет модификатор доступа "по умолчанию"
		Method method = AppSettings.class.getDeclaredMethod("init",
				Properties.class);
		method.setAccessible(true);
		method.invoke(null, params);

		setDba(new MySQLAdaptor());
		setScore(new Score(SCORE_NAME));
	}

}

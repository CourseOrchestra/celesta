package ru.curs.celesta.dbutils.adaptors;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Properties;

import ru.curs.celesta.AppSettings;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.InitTest;
import ru.curs.celesta.dbutils.adaptors.AbstractAdaptorTest;
import ru.curs.celesta.dbutils.adaptors.PostgresAdaptor;
import ru.curs.celesta.score.Score;

public class PostgresAdaptorTest extends AbstractAdaptorTest {

	public PostgresAdaptorTest() throws Exception {
		ConnectionPool.clear();
		Properties params = new Properties();
		InputStream is = InitTest.class
				.getResourceAsStream("celesta.postgres.properties");
		params.load(is);
		// Инициализация параметров приложения: вызов AppSettings.init(params) -
		// метод имеет модификатор доступа "по умолчанию"
		Method method = AppSettings.class.getDeclaredMethod("init",
				Properties.class);
		method.setAccessible(true);
		method.invoke(null, params);

		setDba(new PostgresAdaptor());
		setScore(new Score(SCORE_NAME));
	}

}

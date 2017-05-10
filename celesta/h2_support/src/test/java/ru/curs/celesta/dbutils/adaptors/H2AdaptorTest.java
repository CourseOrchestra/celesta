package ru.curs.celesta.dbutils.adaptors;

import ru.curs.celesta.AppSettings;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.InitTest;
import ru.curs.celesta.dbutils.adaptors.AbstractAdaptorTest;
import ru.curs.celesta.dbutils.adaptors.H2Adaptor;
import ru.curs.celesta.score.Score;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Properties;

/**
 * Created by ioann on 03.05.2017.
 */
public class H2AdaptorTest extends AbstractAdaptorTest {

  public H2AdaptorTest() throws Exception {
    ConnectionPool.clear();
    Properties params = new Properties();
    InputStream is = InitTest.class
        .getResourceAsStream("celesta.h2.properties");
    params.load(is);
    // Инициализация параметров приложения: вызов AppSettings.init(params) -
    // метод имеет модификатор доступа "по умолчанию"
    Method method = AppSettings.class.getDeclaredMethod("init",
        Properties.class);
    method.setAccessible(true);
    method.invoke(null, params);

    setDba(new H2Adaptor());
    setScore(new Score(SCORE_NAME));
  }

}

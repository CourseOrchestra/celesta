package ru.curs.celesta;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.python.core.*;
import org.testcontainers.containers.*;
import ru.curs.celesta.unit.TestClass;

import java.util.*;
import java.util.stream.Stream;


/**
 * Created by ioann on 13.09.2017.
 */
public class CelestaScriptsTest {

  static {
    Locale.setDefault(Locale.US);
  }

  public static PostgreSQLContainer postgres = new PostgreSQLContainer();
  public static OracleContainer oracle = new OracleContainer();
  public static MSSQLServerContainer msSql = new MSSQLServerContainer()
          .withDatabaseName("celesta")
          .withCollation("Cyrillic_General_CI_AS");

  static Celesta h2Celesta;
  static Celesta postgresCelesta;
  static Celesta oracleCelesta;
  static Celesta msSqlCelesta;


  static SessionContext sessionContext;

  CallContext context;

  @BeforeAll
  public static void init() throws CelestaException {
    sessionContext = new SessionContext("super", "debug");

    Properties properties = new Properties();
    properties.put("score.path", "score");
    properties.put("h2.in-memory", "true");
    h2Celesta = Celesta.createInstance(properties);

    postgres.start();
    postgresCelesta = createCelestaByContainer(postgres);

    oracle.start();
    oracleCelesta = createCelestaByContainer(oracle);

    msSql.start();
    msSqlCelesta = createCelestaByContainer(msSql);
  }

  @AfterEach
  public void tearDown() throws CelestaException {
    if (context != null) {
      context.close();
    }
  }

  @AfterAll
  public static void destroy() throws Exception {
    h2Celesta.connectionPool.get().createStatement().execute("SHUTDOWN");
    h2Celesta.close();
    postgresCelesta.close();
    oracleCelesta.close();
    msSqlCelesta.close();
  }

  @DisplayName("Test jython scripts")
  @ParameterizedTest(name = "{1} ==> {2}:{3}")
  @ArgumentsSource(JythonTestProvider.class)
  public void testScripts(JythonTestProvider.TestContext testContext, AppSettings.DBType dbType,
                          String testClassName, String testMethod) throws Exception {
    context = testContext.celesta.callContext(sessionContext);
    testContext.testInstance.__setattr__("context", Py.java2py(context));
    testContext.testInstance.invoke(testContext.testMethod);
  }


  private static Celesta createCelestaByContainer(JdbcDatabaseContainer container) throws CelestaException {
    Properties properties = new Properties();

    properties.put("score.path", "score");
    properties.put("rdbms.connection.url", container.getJdbcUrl());
    properties.put("rdbms.connection.username", container.getUsername());
    properties.put("rdbms.connection.password", container.getPassword());
    properties.put("force.dbinitialize", "true");

    return Celesta.createInstance(properties);
  }

  static class JythonTestProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
      Stream<Arguments> result = Stream.empty();

      result = Stream.concat(result, prepareStreamOfArguments(h2Celesta));
      result = Stream.concat(result, prepareStreamOfArguments(postgresCelesta));
      result = Stream.concat(result, prepareStreamOfArguments(oracleCelesta));
      result = Stream.concat(result, prepareStreamOfArguments(msSqlCelesta));

      return result;
    }

    private Stream<Arguments> prepareStreamOfArguments(Celesta celesta) throws CelestaException {
      AppSettings.DBType dbType = new AppSettings(celesta.getSetupProperties()).getDBType();
      return TestClass.testTypesAndTheirMethods.entrySet().stream()
              .flatMap(e -> {
                PyObject testInstance = e.getKey().__call__();
                return e.getValue().stream()
                        .map(testMethod -> {
                          TestContext testContext = new TestContext(celesta, testInstance, testMethod);
                          String testClassName = testInstance.getType().getName();
                          return Arguments.of(testContext, dbType, testClassName, testMethod);
                        });
              });
    }

    private static class TestContext {
      private Celesta celesta;
      private PyObject testInstance;
      private String testMethod;

      private TestContext(Celesta celesta, PyObject testInstance, String testMethod) {
        this.celesta = celesta;
        this.testInstance = testInstance;
        this.testMethod = testMethod;
      }
    }
  }
}

package ru.curs.celesta.unit;

import org.junit.jupiter.api.*;
import org.python.core.*;
import ru.curs.celesta.*;

import java.sql.Connection;
import java.util.*;
import java.util.stream.Stream;

/**
 * Created by ioann on 13.09.2017.
 */
public class CelestaScriptsTest {

  static Celesta celesta;
  static SessionContext sessionContext;
  static CallContext globalCallContext;

  CallContext context;

  public static Map<PyType, List<String>> testTypesAndTheirMethods = new LinkedHashMap<>();

  public CelestaScriptsTest() {
    System.out.println("NEW CELESTAUNIT!!!");
  }

  @BeforeAll
  public static void init() throws CelestaException {
    Properties properties = new Properties();

    properties.put("score.path", "score");
    properties.put("h2.in-memory", "true");

    //properties.put("database.connection", "jdbc:postgresql://127.0.0.1:5432/celesta?user=postgres&password=123");
    //properties.put("database.connection", "jdbc:oracle:thin:celesta/123@localhost:1521:XE");
    //properties.put("database.connection", "jdbc:sqlserver://localhost;databaseName=celestaunit;user=sa;password=123");

    celesta = Celesta.createInstance(properties);
    sessionContext = new SessionContext("super", "debug");
    globalCallContext = celesta.callContext(sessionContext);
  }

  @BeforeEach
  public void setUp() throws CelestaException {
    context = celesta.callContext(sessionContext);
  }

  @AfterEach
  public void tearDown() throws CelestaException {
    context.close();
  }

  @AfterAll
  public static void destroy() throws CelestaException {
    globalCallContext.close();
    celesta.close();
  }

  @TestFactory
  public Stream<DynamicTest> testScripts() {
    return testTypesAndTheirMethods.entrySet().stream()
        .flatMap(e -> {
          PyObject pyInstance = e.getKey().__call__();
          pyInstance.__setattr__("context", Py.java2py(context));
          return e.getValue().stream()
              .map(method -> DynamicTest.dynamicTest(method, () -> {
                System.out.println("Running " + method);
                pyInstance.invoke(method);
              }));
        });
  }

}

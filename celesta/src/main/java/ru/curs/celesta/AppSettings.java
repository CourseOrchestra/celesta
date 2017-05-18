package ru.curs.celesta;

import java.io.*;
import java.util.Properties;
import java.util.logging.*;

/**
 * Класс, хранящий параметры приложения. Разбирает .properties-файл.
 */
public final class AppSettings {
  private static final String DEFAULT_PYLIB_PATH = "pylib";
  private static AppSettings theSettings;

  private final Properties properties;

  private final String scorePath;
  private final DBType dbType;
  private final String databaseConnection;
  private final boolean h2InMemory;
  private final boolean h2ReferentialIntegrity;
  private final String login;
  private final String password;
  private final Logger logger;
  private final String pylibPath;
  private final String javalibPath;
  private final boolean skipDBUpdate;
  private final boolean forceDBInitialize;
  private final boolean logLogins;

  {
    logger = Logger.getLogger("ru.curs.flute");
    logger.setLevel(Level.INFO);
  }

  private AppSettings(Properties settings) throws CelestaException {
    properties = settings;

    StringBuffer sb = new StringBuffer();

    // Read the settings and check them as thoroughly as possible at this
    // point.

    scorePath = settings.getProperty("score.path", "").trim();
    if (scorePath.isEmpty())
      sb.append("No score path given (score.path).\n");
    else {
      checkEntries(scorePath, "score.path", sb);
    }


    h2ReferentialIntegrity = Boolean.parseBoolean(settings.getProperty("h2.referential.integrity", "false"));
    h2InMemory = Boolean.parseBoolean(settings.getProperty("h2.in-memory", ""));

    //Если настройка h2.in-memory установлена в true - игнорируем настройку строки jdbc подключения и вводим свою
    if (h2InMemory) {
      databaseConnection = "jdbc:h2:mem:celesta;DB_CLOSE_DELAY=-1";
      login = "";
      password = "";
    } else {
      String url = settings.getProperty("database.connection", "").trim();
      if ("".equals(url))
        url = settings.getProperty("rdbms.connection.url", "").trim();
      databaseConnection = url;
      login = settings.getProperty("rdbms.connection.username", "").trim();
      password = settings.getProperty("rdbms.connection.password", "").trim();

      if ("".equals(databaseConnection))
        sb.append("No JDBC URL given (rdbms.connection.url).\n");
    }

    dbType = internalGetDBType(databaseConnection);
    if (dbType == DBType.UNKNOWN)
      sb.append("Cannot recognize RDBMS type or unsupported database.");

    String lf = settings.getProperty("log.file");
    if (lf != null)
      try {
        FileHandler fh = new FileHandler(lf, true);
        fh.setFormatter(new SimpleFormatter());
        logger.addHandler(fh);
      } catch (IOException e) {
        sb.append("Could not access or create log file " + lf + '\n');
      }

    pylibPath = settings.getProperty("pylib.path", DEFAULT_PYLIB_PATH).trim();
    checkEntries(pylibPath, "pylib.path", sb);

    javalibPath = settings.getProperty("javalib.path", "").trim();
    checkEntries(javalibPath, "javalib.path", sb);

    skipDBUpdate = Boolean.parseBoolean(settings.getProperty("skip.dbupdate", "").trim());
    forceDBInitialize = Boolean.parseBoolean(settings.getProperty("force.dbinitialize", "").trim());
    logLogins = Boolean.parseBoolean(settings.getProperty("log.logins", "").trim());

    if (sb.length() > 0)
      throw new CelestaException(sb.toString());

  }

  private static void checkEntries(String path, String propertyName, StringBuffer sb) {
    if (!path.isEmpty())
      for (String pathEntry : path.split(File.pathSeparator)) {
        File pathFile = new File(pathEntry);
        if (!(pathFile.isDirectory() && pathFile.canRead())) {
          sb.append(String.format("Invalid %s entry: %s%n", propertyName, pathEntry));
        }
      }
  }

  /**
   * Initializes AppSettings with given properties.
   *
   * @param settings properties for AppSettings to initialize
   * @throws CelestaException wrong properties format
   */
  public static void init(Properties settings) throws CelestaException {
    theSettings = new AppSettings(settings);
  }

  /**
   * Тип базы данных.
   */
  public enum DBType {
    /**
     * Postgre.
     */
    POSTGRES {
      @Override
      String getDriverClassName() {
        return "org.postgresql.Driver";
      }
    },
    /**
     * MS SQL.
     */
    MSSQL {
      @Override
      String getDriverClassName() {
        return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
      }
    },
    /**
     * ORACLE.
     */
    ORACLE {
      @Override
      String getDriverClassName() {
        return "oracle.jdbc.driver.OracleDriver";
      }
    },
    /**
     * H2
     */
    H2 {
      @Override
      String getDriverClassName() {
        return "org.h2.Driver";
      }
    },
    /**
     * Неизвестный тип.
     */
    UNKNOWN {
      @Override
      String getDriverClassName() {
        return "";
      }
    };

    abstract String getDriverClassName();
  }

  private static DBType internalGetDBType(String url) {
    if (url.startsWith("jdbc:sqlserver")) {
      return DBType.MSSQL;
    } else if (url.startsWith("jdbc:postgresql")) {
      return DBType.POSTGRES;
    } else if (url.startsWith("jdbc:oracle")) {
      return DBType.ORACLE;
    } else if (url.startsWith("jdbc:h2")) {
      return DBType.H2;
    } else {
      return DBType.UNKNOWN;
    }
  }

  /**
   * Возвращает тип базы данных на основе JDBC-строки подключения.
   */
  public static DBType getDBType() {
    return theSettings.dbType;
  }

  /**
   * Возвращает логгер, в который можно записывать сообщения.
   */
  public static Logger getLogger() {
    return theSettings.logger;
  }

  /**
   * Значение параметра "pylib.path".
   */
  public static String getPylibPath() {
    return theSettings.pylibPath;
  }

  /**
   * Значение параметра "javalib.path".
   */

  public static String getJavalibPath() {
    return theSettings.javalibPath;
  }

  /**
   * Значение параметра "пропускать фазу обновления базы данных".
   */
  public static boolean getSkipDBUpdate() {
    return theSettings.skipDBUpdate;
  }

  /**
   * Значение параметра "заставлять обновлять непустую базу данных".
   */
  public static boolean getForceDBInitialize() {
    return theSettings.forceDBInitialize;
  }

  /**
   * Значение параметра "логировать входы и выходы пользователей".
   */
  public static boolean getLogLogins() {
    return theSettings.logLogins;
  }

  /**
   * Значение параметра "score.path".
   */
  public static String getScorePath() {
    return theSettings.scorePath;
  }

  /**
   * Значение параметра "Класс JDBC-подключения".
   */
  public static String getDbClassName() {
    return theSettings.dbType.getDriverClassName();
  }

  /**
   * Значение параметра "Строка JDBC-подключения".
   */
  public static String getDatabaseConnection() {
    return theSettings.databaseConnection;
  }

  /**
   * Флаг поддержки для uniq constraint (отключение позволяет, например,
   * вставлять записи без наличия ссылок на обязательные внешние записи)
   */
  public static boolean isH2ReferentialIntegrity() {
    return theSettings.h2ReferentialIntegrity;
  }

  /**
   * Логин к базе данных.
   */
  public static String getDBLogin() {
    return theSettings.login;
  }

  /**
   * Пароль к базе данных.
   */
  public static String getDBPassword() {
    return theSettings.password;
  }

  /**
   * Возвращает свойства, с которыми была инициализирована Челеста. Внимание:
   * данный объект имеет смысл использовать только на чтение, динамическое
   * изменение этих свойств не приводит ни к чему.
   */
  public static Properties getSetupProperties() {
    return theSettings.properties;
  }
}

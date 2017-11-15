package ru.curs.celesta;

import java.io.*;
import java.util.Properties;
import java.util.logging.*;

/**
 * Класс, хранящий параметры приложения. Разбирает .properties-файл.
 */
public final class AppSettings {
  private static final String DEFAULT_PYLIB_PATH = "pylib";

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

  public AppSettings(Properties properties) throws CelestaException {
    this.properties = properties;

    StringBuffer sb = new StringBuffer();

    // Read the settings and check them as thoroughly as possible at this
    // point.

    scorePath = properties.getProperty("score.path", "").trim();
    if (scorePath.isEmpty())
      sb.append("No score path given (score.path).\n");
    else {
      checkEntries(scorePath, "score.path", sb);
    }


    h2ReferentialIntegrity = Boolean.parseBoolean(properties.getProperty("h2.referential.integrity", "false"));
    h2InMemory = Boolean.parseBoolean(properties.getProperty("h2.in-memory", "false"));

    //Если настройка h2.in-memory установлена в true - игнорируем настройку строки jdbc подключения и вводим свою
    if (h2InMemory) {
      databaseConnection = "jdbc:h2:mem:celesta;DB_CLOSE_DELAY=-1";
      login = "";
      password = "";
    } else {
      String url = properties.getProperty("database.connection", "").trim();
      if ("".equals(url))
        url = properties.getProperty("rdbms.connection.url", "").trim();
      databaseConnection = url;
      login = properties.getProperty("rdbms.connection.username", "").trim();
      password = properties.getProperty("rdbms.connection.password", "").trim();

      if ("".equals(databaseConnection))
        sb.append("No JDBC URL given (rdbms.connection.url).\n");
    }

    dbType = resolveDbType(databaseConnection);
    if (dbType == DBType.UNKNOWN)
      sb.append("Cannot recognize RDBMS type or unsupported database.");

    String lf = properties.getProperty("log.file");
    if (lf != null)
      try {
        FileHandler fh = new FileHandler(lf, true);
        fh.setFormatter(new SimpleFormatter());
        logger.addHandler(fh);
      } catch (IOException e) {
        sb.append("Could not access or create log file " + lf + '\n');
      }

    pylibPath = properties.getProperty("pylib.path", DEFAULT_PYLIB_PATH).trim();
    checkEntries(pylibPath, "pylib.path", sb);

    javalibPath = properties.getProperty("javalib.path", "").trim();
    checkEntries(javalibPath, "javalib.path", sb);

    skipDBUpdate = Boolean.parseBoolean(properties.getProperty("skip.dbupdate", "").trim());
    forceDBInitialize = Boolean.parseBoolean(properties.getProperty("force.dbinitialize", "").trim());
    logLogins = Boolean.parseBoolean(properties.getProperty("log.logins", "").trim());

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
   * Тип базы данных.
   */
  public enum DBType {
    /**
     * Postgre.
     */
    POSTGRES {
      @Override
      public String getDriverClassName() {
        return "org.postgresql.Driver";
      }
    },
    /**
     * MS SQL.
     */
    MSSQL {
      @Override
      public String getDriverClassName() {
        return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
      }
    },
    /**
     * ORACLE.
     */
    ORACLE {
      @Override
      public String getDriverClassName() {
        return "oracle.jdbc.driver.OracleDriver";
      }
    },
    /**
     * H2
     */
    H2 {
      @Override
      public String getDriverClassName() {
        return "org.h2.Driver";
      }
    },
    /**
     * Неизвестный тип.
     */
    UNKNOWN {
      @Override
      public String getDriverClassName() {
        return "";
      }
    };

    abstract public String getDriverClassName();
  }

  public static DBType resolveDbType(String url) {
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
  public DBType getDBType() {
    return dbType;
  }

  /**
   * Возвращает логгер, в который можно записывать сообщения.
   */
  public Logger getLogger() {
    return logger;
  }

  /**
   * Значение параметра "pylib.path".
   */
  public String getPylibPath() {
    return pylibPath;
  }

  /**
   * Значение параметра "javalib.path".
   */

  public String getJavalibPath() {
    return javalibPath;
  }

  /**
   * Значение параметра "пропускать фазу обновления базы данных".
   */
  public boolean getSkipDBUpdate() {
    return skipDBUpdate;
  }

  /**
   * Значение параметра "заставлять обновлять непустую базу данных".
   */
  public boolean getForceDBInitialize() {
    return forceDBInitialize;
  }

  /**
   * Значение параметра "логировать входы и выходы пользователей".
   */
  public boolean getLogLogins() {
    return logLogins;
  }

  /**
   * Значение параметра "score.path".
   */
  public String getScorePath() {
    return scorePath;
  }

  /**
   * Значение параметра "Класс JDBC-подключения".
   */
  public String getDbClassName() {
    return dbType.getDriverClassName();
  }

  /**
   * Значение параметра "Строка JDBC-подключения".
   */
  public String getDatabaseConnection() {
    return databaseConnection;
  }

  /**
   * Флаг поддержки для uniq constraint (отключение позволяет, например,
   * вставлять записи без наличия ссылок на обязательные внешние записи)
   */
  public boolean isH2ReferentialIntegrity() {
    return h2ReferentialIntegrity;
  }

  /**
   * Логин к базе данных.
   */
  public String getDBLogin() {
    return login;
  }

  /**
   * Пароль к базе данных.
   */
  public String getDBPassword() {
    return password;
  }

  /**
   * Возвращает свойства, с которыми была инициализирована Челеста. Внимание:
   * данный объект имеет смысл использовать только на чтение, динамическое
   * изменение этих свойств не приводит ни к чему.
   */
  public Properties getSetupProperties() {
    return properties;
  }
}

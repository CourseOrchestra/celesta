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

	private final String scorePath;
	private final DBType dbType;
	private final String databaseConnection;
	private final String login;
	private final String password;
	private final Logger logger;
	private final String pylibPath;
	private final boolean skipDBUpdate;
	{
		logger = Logger.getLogger("ru.curs.flute");
		logger.setLevel(Level.INFO);
	}

	private AppSettings(Properties settings) throws CelestaException {

		StringBuffer sb = new StringBuffer();

		// Читаем настройки и проверяем их насколько возможно на данном этапе.

		scorePath = settings.getProperty("score.path", "").trim();
		if ("".equals(scorePath))
			sb.append("No score path given (score.path).\n");
		else {
			for (String pathEntry : scorePath.split(";")) {
				File path = new File(pathEntry);
				if (!(path.isDirectory() && path.canRead())) {
					sb.append("Invalid score.path entry: " + pathEntry + '\n');
				}
			}
		}

		String url = settings.getProperty("database.connection", "").trim();
		if ("".equals(url))
			url = settings.getProperty("rdbms.connection.url", "").trim();
		databaseConnection = url;
		login = settings.getProperty("rdbms.connection.username", "").trim();
		password = settings.getProperty("rdbms.connection.password", "").trim();

		if ("".equals(databaseConnection))
			sb.append("No JDBC URL given (rdbms.connection.url).\n");
		dbType = internalGetDBType(url);
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

		pylibPath = settings.getProperty("pylib.path", DEFAULT_PYLIB_PATH);
		File pylibPathFile = new File(pylibPath);
		if (!pylibPathFile.exists())
			sb.append("Invalid pylib.path entry: " + pylibPath + '\n');

		skipDBUpdate = Boolean.parseBoolean(settings.getProperty(
				"skip.dbupdate", "").trim());

		if (sb.length() > 0)
			throw new CelestaException(sb.toString());

	}

	static void init(Properties settings) throws CelestaException {
		theSettings = new AppSettings(settings);
	}

	/**
	 * Тип базы данных.
	 * 
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
		 * MySQL.
		 */
		MYSQL {
			@Override
			String getDriverClassName() {
				return "com.mysql.jdbc.Driver";
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
		} else if (url.startsWith("jdbc:mysql")) {
			return DBType.MYSQL;
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
	 * Значение параметра "пропускать фазу обновления базы данных".
	 */
	public static Boolean getSkipDBUpdate() {
		return theSettings.skipDBUpdate;
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

}

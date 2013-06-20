package ru.curs.celesta;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Класс, хранящий параметры приложения. Разбирает .properties-файл.
 */
public final class AppSettings {

	private static AppSettings theSettings;

	private final String scorePath;
	private final String dbClassName;
	private final String databaseConnection;
	private final Logger logger;
	{
		logger = Logger.getLogger("ru.curs.flute");
		logger.setLevel(Level.INFO);
	}

	private AppSettings(Properties settings) throws CelestaCritical {

		StringBuffer sb = new StringBuffer();

		// Читаем настройки и проверяем их насколько возможно на данном этапе.

		scorePath = settings.getProperty("score.path", "");
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

		dbClassName = settings.getProperty("database.classname", "");
		if ("".equals(dbClassName))
			sb.append("No JDBC driver class name given (database.classname).\n");

		databaseConnection = settings.getProperty("database.connection", "");
		if ("".equals(databaseConnection))
			sb.append("No JDBC URL given (database.connection).\n");
		if (internalGetDBType() == DBType.UNKNOWN)
			sb.append("Cannot recognize RDBMS type.");

		String lf = settings.getProperty("log.file");
		if (lf != null)
			try {
				FileHandler fh = new FileHandler(lf, true);
				fh.setFormatter(new SimpleFormatter());
				logger.addHandler(fh);
			} catch (IOException e) {
				sb.append("Could not access or create log file " + lf + '\n');
			}

		if (sb.length() > 0)
			throw new CelestaCritical(sb.toString());

	}

	static void init(Properties settings) throws CelestaCritical {
		theSettings = new AppSettings(settings);
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
		return theSettings.dbClassName;
	}

	/**
	 * Значение параметра "Строка JDBC-подключения".
	 */
	public static String getDatabaseConnection() {
		return theSettings.databaseConnection;
	}

	/**
	 * Тип базы данных.
	 * 
	 */
	public enum DBType {
		/**
		 * Postgre.
		 */
		POSTGRES, /**
		 * MS SQL.
		 */
		MSSQL, /**
		 * ORACLE.
		 */
		ORACLE,
		/**
		 * MySQL.
		 */
		MYSQL, /**
		 * Неизвестный тип.
		 */
		UNKNOWN
	}

	private DBType internalGetDBType() {
		if (databaseConnection.startsWith("jdbc:sqlserver")) {
			return DBType.MSSQL;
		} else if (databaseConnection.startsWith("jdbc:postgresql")) {
			return DBType.POSTGRES;
		} else if (databaseConnection.startsWith("jdbc:oracle")) {
			return DBType.ORACLE;
		} else if (databaseConnection.startsWith("jdbc:mysql")) {
			return DBType.MYSQL;
		} else {
			return DBType.UNKNOWN;
		}
	}

	/**
	 * Возвращает тип базы данных на основе JDBC-строки подключения.
	 */
	public static DBType getDBType() {
		return theSettings.internalGetDBType();
	}

	/**
	 * Возвращает логгер, в который можно записывать сообщения.
	 */
	public static Logger getLogger() {
		return theSettings.logger;
	}
}

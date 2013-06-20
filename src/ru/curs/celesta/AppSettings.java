package ru.curs.celesta;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Класс, хранящий параметры приложения. Разбирает .properties-файл.
 */
public final class AppSettings {

	private static AppSettings theSettings;

	private final String scorePath;
	private final String dbClassName;
	private final String databaseConnection;

	private AppSettings(File f) throws CelestaCritical {
		Properties settings = new Properties();
		try {
			FileInputStream in = new FileInputStream(f);
			settings.load(in);
		} catch (IOException e) {
			throw new CelestaCritical("IOException: " + e.getMessage());
		}

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
		if (getDBType() == DBType.UNKNOWN)
			sb.append("Cannot recognize RDBMS type.");

		if (sb.length() > 0)
			throw new CelestaCritical(sb.toString());

	}

	static void init(File f) throws CelestaCritical {
		theSettings = new AppSettings(f);
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

	/**
	 * Возвращает тип базы данных на основе JDBC-строки подключения.
	 */
	public static DBType getDBType() {
		if (theSettings.databaseConnection.startsWith("jdbc:sqlserver")) {
			return DBType.MSSQL;
		} else if (theSettings.databaseConnection.startsWith("jdbc:postgresql")) {
			return DBType.POSTGRES;
		} else if (theSettings.databaseConnection.startsWith("jdbc:oracle")) {
			return DBType.ORACLE;
		} else if (theSettings.databaseConnection.startsWith("jdbc:mysql")) {
			return DBType.MYSQL;
		} else {
			return DBType.UNKNOWN;
		}
	}
}

package ru.curs.celesta;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ru.curs.celesta.dbutils.DBAdaptor;

/**
 * Пул соединений с базой данных.
 * 
 */
public final class ConnectionPool {

	private static final Queue<Connection> POOL = new LinkedList<>();

	private ConnectionPool() {

	}

	/**
	 * Извлекает соединение из пула.
	 * 
	 * @throws CelestaException
	 *             В случае, если новое соединение не удалось создать.
	 */
	public static synchronized Connection get() throws CelestaException {
		Connection c = POOL.poll();
		while (c != null) {
			try {
				if (DBAdaptor.getAdaptor().isValidConnection(c, 1)) {
					return c;
				}
			} catch (CelestaException e) {
				// do something to make CheckStyle happy ))
				c = null;
			}
			c = POOL.poll();
		}
		try {
			Class.forName(AppSettings.getDbClassName());
			c = DriverManager
					.getConnection(AppSettings.getDatabaseConnection());
			c.setAutoCommit(false);
			return c;
		} catch (SQLException | ClassNotFoundException e) {
			throw new CelestaException(
					"Could not connect to %s with error: %s",
					PasswordHider.maskPassword(AppSettings
							.getDatabaseConnection()), e.getMessage());
		}
	}

	/**
	 * Возвращает соединение в пул. Выполняет операцию commit, если до этого она
	 * не была произведена.
	 * 
	 * @param c
	 *            возвращаемое соединение.
	 */
	public static synchronized void putBack(Connection c) {
		// Вставляем только хорошие соединения...
		try {
			if (c != null && DBAdaptor.getAdaptor().isValidConnection(c, 1)) {
				c.commit();
				POOL.add(c);
			}
		} catch (SQLException | CelestaException e) {
			// do something to make CheckStyle happy ))
			e.printStackTrace();
			return;
		}
	}

	/**
	 * Выполняет команду commit на коннекшне, не выдавая исключения.
	 * 
	 * @param conn
	 *            соединение для выполнения коммита.
	 */
	public static void commit(Connection conn) {
		try {
			if (conn != null)
				conn.commit();
		} catch (SQLException e) {
			// do something to make CheckStyle happy ))
			return;
		}
	}

	/**
	 * Очищает пул.
	 */
	public static synchronized void clear() {
		Connection c = POOL.poll();
		while (c != null) {
			try {
				c.close();
			} catch (SQLException e) {
				c = null;
			}
			c = POOL.poll();
		}
	}
}

/**
 * Класс-утилита для сокрытия пароля в строке JDBC-подключения. Скопировано из
 * проекта FormsServer.
 * 
 */
final class PasswordHider {
	// Пароль Oracle всегда между / и @ (и не может содержать @).
	private static final Pattern ORA_PATTERN = Pattern.compile("/[^@]+@");
	// В MS SQL всё продумано и если пароль содержит ;, она меняется на {;}
	private static final Pattern MSSQL_PATTERN = Pattern.compile(
			"(password)=([^{;]|(\\{(;\\})|[^;]?))+(;|$)",
			Pattern.CASE_INSENSITIVE);
	// В MySQL JDBC-URL не сработает правильно, если пароль содержит &
	private static final Pattern MYSQL_PATTERN = Pattern.compile(
			"(password)=[^&]+(&|$)", Pattern.CASE_INSENSITIVE);
	// А это на случай неизвестного науке JDBC-драйвера
	private static final Pattern GENERIC_PATTERN = Pattern.compile(
			"(password)=.+$", Pattern.CASE_INSENSITIVE);

	private PasswordHider() {

	}

	/**
	 * 
	 * Метод, маскирующий пароль в строке JDBC-подключения.
	 * 
	 * @param url
	 *            Строка, содержащая URL JDBC-подключения
	 * 
	 */
	public static String maskPassword(String url) {
		if (url == null)
			return null;
		Matcher m;
		StringBuffer sb = new StringBuffer();
		if (url.toLowerCase().startsWith("jdbc:oracle")) {
			m = ORA_PATTERN.matcher(url);
			while (m.find()) {
				m.appendReplacement(sb, "/*****@");
			}
		} else if (url.toLowerCase().startsWith("jdbc:sqlserver")) {
			m = MSSQL_PATTERN.matcher(url);
			while (m.find()) {
				m.appendReplacement(sb, m.group(1) + "=*****" + m.group(5));
			}
		} else if (url.toLowerCase().startsWith("jdbc:mysql")
				|| url.toLowerCase().startsWith("jdbc:postgresql")) {
			m = MYSQL_PATTERN.matcher(url);
			while (m.find()) {
				m.appendReplacement(sb, m.group(1) + "=*****" + m.group(2));
			}
		} else {
			m = GENERIC_PATTERN.matcher(url);
			while (m.find()) {
				m.appendReplacement(sb, m.group(1) + "=*****");
			}
		}
		m.appendTail(sb);
		return sb.toString();
	}
}

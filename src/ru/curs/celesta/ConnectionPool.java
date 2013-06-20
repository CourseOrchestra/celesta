package ru.curs.celesta;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Queue;

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
	 * @throws CelestaCritical
	 *             В случае, если новое соединение не удалось создать.
	 */
	public static synchronized Connection get() throws CelestaCritical {
		Connection c = POOL.poll();
		while (c != null) {
			try {
				if (c.isValid(1)) {
					return c;
				}
			} catch (SQLException e) {
				// do something to make CheckStyle happy ))
				c = null;
			}
			c = POOL.poll();
		}
		try {
			return DriverManager.getConnection(AppSettings
					.getDatabaseConnection());
		} catch (SQLException e) {
			throw new CelestaCritical("Could not connect to "
					+ AppSettings.getDatabaseConnection() + "with error: "
					+ e.getMessage());
		}

	}

	/**
	 * Возвращает соединение в пул.
	 * 
	 * @param c
	 *            возвращаемое соединение.
	 */
	public static synchronized void putBack(Connection c) {
		// Вставляем только хорошие соединения...
		try {
			if (c != null && !c.isValid(1))
				POOL.add(c);
		} catch (SQLException e) {
			// do something to make CheckStyle happy ))
			return;
		}
	}

}

package ru.curs.celesta;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Контекст вызова, содержащий несущее транзакцию соединение с БД и
 * идентификатор пользователя.
 */
public final class CallContext {

	private final Connection conn;
	private final String userId;
	private final Celesta celesta;

	public CallContext(Connection conn, String userId, Celesta celesta) {
		this.conn = conn;
		this.userId = userId;
		this.celesta = celesta;
	}

	/**
	 * Соединение с базой данных.
	 */
	public Connection getConn() {
		return conn;
	}

	/**
	 * Идентификатор пользователя.
	 */
	public String getUserId() {
		return userId;
	}

	/**
	 * Коммитит транзакцию.
	 * 
	 * @throws CelestaException
	 *             в случае проблемы с БД.
	 */
	public void commit() throws CelestaException {
		try {
			conn.commit();
		} catch (SQLException e) {
			throw new CelestaException("Commit unsuccessful: %s",
					e.getMessage());
		}
	}

	/**
	 * Возвращает экземпляр Celesta, использованный при создании контекста
	 * вызова.
	 */
	Celesta getCelesta() {
		return celesta;
	}

}

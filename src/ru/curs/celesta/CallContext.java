package ru.curs.celesta;

import java.sql.Connection;

/**
 * Контекст вызова, содержащий несущее транзакцию соединение с БД и
 * идентификатор пользователя.
 */
public class CallContext {

	private final Connection conn;
	private final String userId;

	public CallContext(Connection conn, String userId) {
		this.conn = conn;
		this.userId = userId;
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

}

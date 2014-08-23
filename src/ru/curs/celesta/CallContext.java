package ru.curs.celesta;

import java.sql.*;

import org.python.core.PyDictionary;

/**
 * Контекст вызова, содержащий несущее транзакцию соединение с БД и
 * идентификатор пользователя.
 */
public final class CallContext {

	private final Connection conn;
	private final SessionContext sesContext;

	public CallContext(Connection conn, SessionContext sesContext) {
		this.conn = conn;
		this.sesContext = sesContext;
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
		return sesContext.getUserId();
	}

	/**
	 * Идентификатор сессии.
	 */
	public String getSessionId() {
		return sesContext.getSessionId();
	}

	/**
	 * Данные сессии.
	 */
	public PyDictionary getData() {
		return sesContext.getData();
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
	 * Записывает информационное сообщение в очередь сообщений.
	 * 
	 * @param msg
	 *            текст сообщения
	 */
	public void info(String msg) {
		sesContext.addMessage(new CelestaMessage(CelestaMessage.INFO, msg));
	}

	/**
	 * Записывает предупреждение в очередь сообщений.
	 * 
	 * @param msg
	 *            текст сообщения
	 */
	public void warning(String msg) {
		sesContext.addMessage(new CelestaMessage(CelestaMessage.WARNING, msg));
	}

	/**
	 * Записывает ошибку в очередь сообщений и вызывает исключение.
	 * 
	 * @param msg
	 *            текст сообщения
	 * @throws CelestaException
	 *             во всех случаях, при этом с переданным текстом
	 */
	public void error(String msg) throws CelestaException {
		sesContext.addMessage(new CelestaMessage(CelestaMessage.ERROR, msg));
		throw new CelestaException("ERROR: %s", msg);
	}

	/**
	 * Возвращает экземпляр Celesta, использованный при создании контекста
	 * вызова.
	 * 
	 * @throws CelestaException
	 *             ошибка инициализации Celesta
	 */
	public Celesta getCelesta() throws CelestaException {
		return Celesta.getInstance();
	}

}

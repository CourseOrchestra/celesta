package ru.curs.celesta;

import org.python.core.PyDictionary;

/**
 * Контекст сессии.
 */
public final class SessionContext {
	private final String userId;
	private final String sessionId;
	private final PyDictionary data = new PyDictionary();

	public SessionContext(String userId, String sessionId) {
		this.userId = userId;
		this.sessionId = sessionId;
	}

	/**
	 * Данные контекста сессии.
	 */
	public PyDictionary getData() {
		return data;
	}

	/**
	 * Имя пользователя.
	 */
	public String getUserId() {
		return userId;
	}

	/**
	 * Идентификатор сессии пользователя.
	 */
	public String getSessionId() {
		return sessionId;
	}
}
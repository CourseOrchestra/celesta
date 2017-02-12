package ru.curs.celesta;

import org.python.core.Py;
import org.python.core.PyDictionary;
import org.python.core.PyString;

/**
 * Контекст сессии.
 */
public final class SessionContext {
	// Session expiration time (40 minutes by default)
	private final static long EXPIRATION_TIME = 40 * 60 * 1000;
	private final String userId;
	private final String sessionId;
	private final PyDictionary data = new PyDictionary();
	private CelestaMessage.MessageReceiver receiver = null;

	private long expirationTime;

	public SessionContext(String userId, String sessionId) {
		this.userId = userId;
		this.sessionId = sessionId;
		touch();
	}

	void touch() {
		expirationTime = System.currentTimeMillis() + EXPIRATION_TIME;
	}

	boolean isExpired() {
		return expirationTime < System.currentTimeMillis();
	}

	void removeForms() {
		data.pop(new PyString("_lyraForms"), Py.None);
	}

	void addMessage(CelestaMessage msg) {
		if (receiver != null)
			receiver.receive(msg);
	}

	/**
	 * Устанавливает приёмник сообщений.
	 * 
	 * @param receiver
	 *            приёмник сообщений.
	 */
	public void setMessageReceiver(CelestaMessage.MessageReceiver receiver) {
		this.receiver = receiver;
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
package ru.curs.celesta;

import org.python.core.Py;
import org.python.core.PyDictionary;
import org.python.core.PyString;

/**
 * Контекст сессии.
 */
public final class SessionContext {
	private final String userId;
	private final String sessionId;
	private final PyDictionary data = new PyDictionary();
	private CelestaMessage.MessageReceiver receiver = null;

	public SessionContext(String userId, String sessionId) {
		this.userId = userId;
		this.sessionId = sessionId;
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
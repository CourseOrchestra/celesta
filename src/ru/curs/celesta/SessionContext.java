package ru.curs.celesta;

import java.util.LinkedList;

import org.python.core.PyDictionary;

/**
 * Контекст сессии.
 */
public final class SessionContext {
	private static final int MAX_MESSAGE_COUNT = 16;
	private final String userId;
	private final String sessionId;
	private final PyDictionary data = new PyDictionary();
	private final LinkedList<CelestaMessage> messages = new LinkedList<>();

	public SessionContext(String userId, String sessionId) {
		this.userId = userId;
		this.sessionId = sessionId;
	}

	void addMessage(CelestaMessage msg) {
		messages.add(msg);
		while (messages.size() > MAX_MESSAGE_COUNT) {
			messages.poll();
		}
	}

	CelestaMessage pollMessage() {
		return messages.poll();
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
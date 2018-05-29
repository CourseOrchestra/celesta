package ru.curs.celesta;

import org.python.core.Py;
import org.python.core.PyDictionary;
import org.python.core.PyString;

/**
 * Контекст сессии.
 */
public final class PySessionContext extends SessionContext {

	private final PyDictionary data = new PyDictionary();
	private CelestaMessage.MessageReceiver receiver = null;

	public PySessionContext(String userId, String sessionId) {
		super(userId, sessionId);
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

	@Override
	protected CallContext.CallContextBuilder callContextBuilder() {
		return PyCallContext.builder();
	}
}
package ru.curs.celesta.showcase;

import java.io.Serializable;

/**
 * Класс для результатов добавления записи в гриде.
 * 
 */
public class GridAddRecordResult implements Serializable {
	private static final long serialVersionUID = 4793925085602167821L;

	/**
	 * "ok"-сообщение.
	 */
	private UserMessage okMessage = null;

	public GridAddRecordResult() {
		super();
	}

	public GridAddRecordResult(final UserMessage aOkMessage) {
		super();
		okMessage = aOkMessage;
	}

	public UserMessage getOkMessage() {
		return okMessage;
	}

	public void setOkMessage(final UserMessage aOkMessage) {
		okMessage = aOkMessage;
	}

}

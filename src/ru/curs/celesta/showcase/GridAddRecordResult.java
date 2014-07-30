package ru.curs.celesta.showcase;

import javax.xml.bind.annotation.*;

import ru.beta2.extra.gwt.ui.SerializableElement;

/**
 * Класс для результатов добавления записи в гриде.
 * 
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class GridAddRecordResult implements SerializableElement {
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

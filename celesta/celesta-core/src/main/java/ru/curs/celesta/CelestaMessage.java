package ru.curs.celesta;

import java.util.Date;

/**
 * Сообщение от питон-скрипта.
 */
public class CelestaMessage {

	/**
	 * Интерфейс получателя сообщений.
	 */
	@FunctionalInterface
	public interface MessageReceiver {
		/**
		 * Получить сообщение.
		 * 
		 * @param msg
		 *            сообщение.
		 */
		void receive(CelestaMessage msg);
	}

	/**
	 * Вид сообщения "информация".
	 */
	public static final int INFO = 0;
	/**
	 * Вид сообщения "предупреждение".
	 */
	public static final int WARNING = 1;
	/**
	 * Вид сообщения "ошибка".
	 */
	public static final int ERROR = 2;

	private final Date timestamp = new Date();
	private final int kind;
	private final String message;
	private final String caption;
	private final String subkind;

	public CelestaMessage(int kind, String message, String caption, String subkind) {
		if (!(kind == INFO || kind == WARNING || kind == ERROR))
			throw new IllegalArgumentException("Invalid message kind value");
		this.kind = kind;
		this.message = message;
		this.caption = caption;
		this.subkind = subkind;
	}

	public CelestaMessage(int kind, String message) {
		this(kind, message, null, null);
	}

	public CelestaMessage(int kind, String message, String caption) {
		this(kind, message, caption, null);
	}

	/**
	 * Время создания сообщения.
	 */
	public Date getTimestamp() {
		return timestamp;
	}

	/**
	 * Тип сообщения.
	 */
	public int getKind() {
		return kind;
	}

	/**
	 * Текст сообщения.
	 */
	public String getMessage() {
		return message;
	}

	/**
	 * Заголовок окна сообщения.
	 */
	public String getCaption() {
		return caption;
	}

	/**
	 * Тип сообщения.
	 */
	public String getSubkind() {
		return subkind;
	}
}

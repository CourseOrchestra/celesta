package ru.curs.celesta;

import java.util.Date;

/**
 * Сообщение от питон-скрипта.
 */
public class CelestaMessage {

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

	public CelestaMessage(int kind, String message) {
		if (!(kind == INFO || kind == WARNING || kind == ERROR))
			throw new IllegalArgumentException("Invalid message kind value");
		this.kind = kind;
		this.message = message;
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

}

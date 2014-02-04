package ru.curs.celesta.score;

/**
 * Тип выражения.
 */
public enum ViewColumnType {
	/**
	 * Логическое условие.
	 */
	LOGIC, /**
	 * Числовое значение.
	 */
	NUMERIC, /**
	 * Текстовое значение.
	 */
	TEXT, /**
	 * Дата.
	 */
	DATE, /**
	 * Булевское значение.
	 */
	BIT, /**
	 * Большой объект.
	 */
	BLOB, /**
	 * Неопределённое значение.
	 */
	UNDEFINED
}
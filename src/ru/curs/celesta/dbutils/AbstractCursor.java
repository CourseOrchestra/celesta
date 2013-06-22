package ru.curs.celesta.dbutils;

/**
 * Базовый класс курсора (аналог соответствующего класса в Python-коде).
 */
abstract class AbstractCursor {

	public AbstractCursor() {

	}

	/**
	 * Очищает все поля в записи, кроме ключевых.
	 */
	public void init() {

	}

	/**
	 * Переходит к первой записи в отсортированном наборе.
	 */
	public void first() {

	}

	/**
	 * Переходит к следующей записи в отсортированном наборе. Возвращает false,
	 * если достигнут конец набора.
	 */
	public boolean next() {
		return false;
	}

	/**
	 * Осуществляет вставку курсора в БД.
	 */
	public void insert() {

	}
}

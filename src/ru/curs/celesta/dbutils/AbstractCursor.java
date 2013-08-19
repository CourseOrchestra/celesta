package ru.curs.celesta.dbutils;

import java.sql.Connection;

import ru.curs.celesta.CelestaCritical;

/**
 * Базовый класс курсора (аналог соответствующего класса в Python-коде).
 */
abstract class AbstractCursor {

	private final Connection conn;

	public AbstractCursor(Connection conn) {
		this.conn = conn;
	}

	/**
	 * Очищает все поля в записи, кроме ключевых.
	 */
	public void init() {

	}

	/**
	 * Переходит к первой записи в отсортированном наборе и возвращает
	 * информацию об успешности перехода.
	 * 
	 * @return true, если переход успешен, false -- если записей в наборе нет.
	 */
	public boolean tryFirst() {
		return false;
	}

	/**
	 * Переходит к первой записи в отсортированном наборе, вызывая ошибку в
	 * случае, если переход неудачен.
	 * 
	 * @throws CelestaCritical
	 *             в случае, если записей в наборе нет.
	 */
	public void first() throws CelestaCritical {
		// if (!tryFirst())
		// throw
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
	public void insert() throws CelestaCritical {

	}

	/**
	 * Осуществляет вставку курсора в БД.
	 */
	public boolean tryInsert() {
		return false;
	}

	/**
	 * Осуществляет сохранение курсора в БД.
	 */
	public void update() throws CelestaCritical {

	}

	/**
	 * Осуществляет сохранение курсора в БД.
	 */
	public boolean tryUpdate() {
		return false;
	}

	/**
	 * Осуществляет поиск записи по ключевым полям.
	 * 
	 * @param values
	 *            значения ключевых полей
	 * @throws CelestaCritical
	 *             в случае, если запись не найдена
	 */
	public void get(Object... values) throws CelestaCritical {

	}

	public boolean tryGet(Object... values) {
		return false;
	}

	public void setFilter(String name, Object value) {

	}

	public void orderBy(String... names) {

	}

}

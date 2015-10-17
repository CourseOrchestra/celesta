package ru.curs.celesta.dbutils;

import java.math.BigInteger;

/**
 * Обратный вызов по завершению асинхронного процесса уточнения позиции движка.
 */
public interface GridRefinementCallback {

	/**
	 * Обработчик результата выполнения процесса уточнения.
	 * 
	 * @param key
	 *            Ключ.
	 * @param result
	 *            Номер записи.
	 */
	void execute(BigInteger key, int result);

}

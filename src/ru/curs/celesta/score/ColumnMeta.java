package ru.curs.celesta.score;

/**
 * Информация о типе столбца таблицы или представления .
 */
public interface ColumnMeta {

	/**
	 * Имя jdbcGetter-а, которое следует использовать для получения данных
	 * столбца.
	 */
	String jdbcGetterName();

	/**
	 * Тип данных Celesta,соответствующий полю.
	 */
	String getCelestaType();
	
	/**
	 * Column's CelestaDoc.
	 */
	String getCelestaDoc();

}

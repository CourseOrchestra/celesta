package ru.curs.celesta.dbutils;

import ru.curs.celesta.AppSettings;
import ru.curs.celesta.CelestaCritical;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.Table;

/**
 * Адаптер соединения с БД, выполняющий команды, необходимые системе обновления.
 * 
 */
abstract class DBAdaptor {

	/**
	 * Возвращает true в том и только том случае, если база данных содержит
	 * таблицу celesta.grains.
	 */
	public abstract boolean tableExists(String schema, String name);

	/**
	 * Возвращает true в том и только том случае, если база данных содержит
	 * пользовательские таблицы (т. е. не является пустой базой данных).
	 */
	public abstract boolean userTablesExist();

	/**
	 * Создаёт в базе данных схему с указанным именем, если таковая схема ранее
	 * не существовала.
	 * 
	 * @param string
	 *            имя схемы.
	 * @throws CelestaCritical
	 *             только в том случае, если возник критический сбой при
	 *             создании схемы. Не выбрасывается в случае, если схема с
	 *             данным именем уже существует в базе данных.
	 */
	public abstract void createSchemaIfNotExists(String string)
			throws CelestaCritical;

	/**
	 * Возвращает наименование типа столбца, соответствующее базе данных.
	 * 
	 * @param c
	 *            Колонка в score
	 */
	abstract String dbFieldType(Column c);

	/**
	 * Фабрика классов адаптеров подходящего под текущие настройки типа.
	 * 
	 * @throws CelestaCritical
	 *             При ошибке создания адаптера (например, при создании адаптера
	 *             не поддерживаемого типа).
	 */
	public static DBAdaptor getAdaptor() throws CelestaCritical {
		switch (AppSettings.getDBType()) {
		case MSSQL:
			return new MSSQLAdaptor();
		case MYSQL:
			return new MySQLAdaptor();
		case ORACLE:
			return new OraAdaptor();
		case POSTGRES:
			return new PostgresAdaptor();
		case UNKNOWN:
		default:
			throw new CelestaCritical("Unknown or unsupported database type.");
		}
	}

	/**
	 * Создаёт в базе данных таблицу "с нуля".
	 * 
	 * @param table
	 *            Таблица для создания.
	 */
	public void createTable(Table table) {
		// TODO Auto-generated method stub

	}

}

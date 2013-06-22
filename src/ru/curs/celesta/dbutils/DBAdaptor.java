package ru.curs.celesta.dbutils;

import ru.curs.celesta.AppSettings;
import ru.curs.celesta.CelestaCritical;

/**
 * Адаптер соединения с БД, выполняющий команды, необходимые системе обновления.
 * 
 */
abstract class DBAdaptor {

	/**
	 * Возвращает true в том и только том случае, если база данных содержит
	 * таблицу celesta.grains.
	 */
	public abstract boolean grainsTableExists();

	/**
	 * Возвращает true в том и только том случае, если база данных содержит
	 * пользовательские таблицы (т. е. не является пустой базой данных).
	 */
	public abstract boolean userTablesExist();

	/**
	 * Создаёт таблицу Grains и другие системные таблицы.
	 */
	public abstract void createSystemTables();

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
}

package ru.curs.celesta.dbutils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import ru.curs.celesta.AppSettings;
import ru.curs.celesta.CelestaCritical;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.Table;

/**
 * Адаптер соединения с БД, выполняющий команды, необходимые системе обновления.
 * 
 */
abstract class DBAdaptor {

	/**
	 * Класс, ответственный за генерацию определения столбца таблицы в разных
	 * СУБД.
	 * 
	 */
	abstract static class ColumnDefiner {
		static final String DEFAULT = "default ";

		abstract String dbFieldType();

		abstract String getColumnDef(Column c);

		String nullable(Column c) {
			return c.isNullable() ? "" : "not null";
		}

		/**
		 * Соединяет строки через пробел.
		 * 
		 * @param ss
		 *            массив строк для соединения в виде свободного параметра.
		 */
		static String join(String... ss) {
			StringBuilder sb = new StringBuilder();
			boolean multiple = false;
			for (String s : ss)
				if (!"".equals(s)) {
					if (multiple)
						sb.append(' ' + s);
					else {
						sb.append(s);
						multiple = true;
					}
				}
			return sb.toString();
		}
	}

	/**
	 * Возвращает true в том и только том случае, если база данных содержит
	 * таблицу.
	 * 
	 * @param schema
	 *            схема.
	 * @param name
	 *            имя таблицы.
	 */
	public final boolean tableExists(String schema, String name)
			throws CelestaCritical {
		Connection conn = ConnectionPool.get();
		try {
			return tableExists(conn, schema, name);
		} catch (SQLException e) {
			throw new CelestaCritical(e.getMessage());
		} finally {
			ConnectionPool.putBack(conn);
		}
	}

	abstract boolean tableExists(Connection conn, String schema, String name)
			throws SQLException;

	/**
	 * Возвращает true в том и только том случае, если база данных содержит
	 * пользовательские таблицы (т. е. не является пустой базой данных).
	 */
	public final boolean userTablesExist() throws CelestaCritical {
		Connection conn = ConnectionPool.get();
		try {
			return userTablesExist(conn);
		} catch (SQLException e) {
			throw new CelestaCritical(e.getMessage());
		} finally {
			ConnectionPool.putBack(conn);
		}
	}

	abstract boolean userTablesExist(Connection conn) throws SQLException;

	/**
	 * Создаёт в базе данных схему с указанным именем, если таковая схема ранее
	 * не существовала.
	 * 
	 * @param name
	 *            имя схемы.
	 * @throws CelestaCritical
	 *             только в том случае, если возник критический сбой при
	 *             создании схемы. Не выбрасывается в случае, если схема с
	 *             данным именем уже существует в базе данных.
	 */
	public final void createSchemaIfNotExists(String name)
			throws CelestaCritical {
		Connection conn = ConnectionPool.get();
		try {
			createSchemaIfNotExists(conn, name);
		} catch (SQLException e) {
			throw new CelestaCritical("Cannot create schema. " + e.getMessage());
		} finally {
			ConnectionPool.putBack(conn);
		}
	}

	abstract void createSchemaIfNotExists(Connection conn, String name)
			throws SQLException;

	/**
	 * Возвращает наименование типа столбца, соответствующее базе данных.
	 * 
	 * @param c
	 *            Колонка в score
	 */
	final String dbFieldType(Column c) {
		return getColumnDefiner(c).dbFieldType();
	}

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

	final String columnDef(Column c) {
		return getColumnDefiner(c).getColumnDef(c);
	}

	String tableDef(Table table) {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("create table %s.%s(\n", table.getGrain()
				.getName(), table.getName()));
		boolean multiple = false;
		for (Column c : table.getColumns().values()) {
			if (multiple)
				sb.append(",\n");
			sb.append("  " + columnDef(c));
			multiple = true;
		}
		sb.append("\n);");
		return sb.toString();
	}

	/**
	 * Создаёт в базе данных таблицу "с нуля".
	 * 
	 * @param table
	 *            Таблица для создания.
	 * @throws CelestaCritical
	 *             В случае возникновения критического сбоя при создании
	 *             таблицы, в том числе в случае, если такая таблица существует.
	 */
	public void createTable(Table table) throws CelestaCritical {
		String def = tableDef(table);
		Connection conn = ConnectionPool.get();
		try {
			PreparedStatement stmt = conn.prepareStatement(def);
			stmt.execute();
			stmt.close();
		} catch (SQLException e) {
			throw new CelestaCritical("Cannot create table. " + e.getMessage());
		} finally {
			ConnectionPool.putBack(conn);
		}
	}

	abstract ColumnDefiner getColumnDefiner(Column c);
}

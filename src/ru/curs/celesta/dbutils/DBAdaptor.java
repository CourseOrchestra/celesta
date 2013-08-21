package ru.curs.celesta.dbutils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;

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

	abstract PreparedStatement getOneRecordStatement(Connection conn, Table t)
			throws CelestaCritical;

	abstract PreparedStatement getRecordSetStatement(Connection conn, Table t,
			Map<String, AbstractFilter> filters, List<String> orderBy)
			throws CelestaCritical;

	abstract PreparedStatement getInsertRecordStatement(Connection conn, Table t)
			throws CelestaCritical;

	abstract PreparedStatement getUpdateRecordStatement(Connection conn, Table t)
			throws CelestaCritical;

	abstract PreparedStatement getDeleteRecordStatement(Connection conn, Table t)
			throws CelestaCritical;

	static PreparedStatement prepareStatement(Connection conn, String sql)
			throws CelestaCritical {
		try {
			return conn.prepareStatement(sql);
		} catch (SQLException e) {
			throw new CelestaCritical(e.getMessage());
		}
	}

	static String getFieldList(Iterable<String> fields) {
		// NB: этот метод возможно нужно будет сделать виртуальным, чтобы учесть
		// особенности синтаксиса разных баз данных
		StringBuilder sb = new StringBuilder();
		for (String c : fields) {
			if (sb.length() > 0)
				sb.append(", ");
			sb.append(c);
		}
		return sb.toString();
	}

	static String getTableFieldsList(Table t) {
		return getFieldList(t.getColumns().keySet());
	}

	static String getSelectFromOrderBy(Table t, String whereClause,
			List<String> orderBy) {
		String sqlfrom = String.format("select %s from %s.%s",
				getTableFieldsList(t), t.getGrain().getName(), t.getName());

		String sqlwhere = "".equals(whereClause) ? "" : " where " + whereClause;

		String orderByList = getFieldList(orderBy);
		String sqlorder = "".equals(orderByList) ? "" : " order by "
				+ orderByList;

		return sqlfrom + sqlwhere + sqlorder + ";";
	}

	static String getRecordWhereClause(Table t) {
		StringBuilder whereClause = new StringBuilder();
		for (String fieldName : t.getPrimaryKey().keySet())
			whereClause.append(String.format("%s(%s = ?)",
					whereClause.length() > 0 ? " and " : "", fieldName));
		return whereClause.toString();
	}

	static void setParam(PreparedStatement stmt, int i, Object v)
			throws CelestaCritical {
		try {
			if (v == null)
				stmt.setNull(i, java.sql.Types.NULL);
			else if (v instanceof Integer)
				stmt.setInt(i, (Integer) v);
			else if (v instanceof Double)
				stmt.setDouble(i, (Double) v);
			else if (v instanceof String)
				stmt.setString(i, (String) v);
			else if (v instanceof Boolean)
				stmt.setBoolean(i, (Boolean) v);
			else if (v instanceof Date) {
				Timestamp d = new Timestamp(((Date) v).getTime());
				stmt.setTimestamp(i, d);
			} else {
				throw new CelestaCritical(
						"You can filter only on Integer, Double, String, "
								+ "Boolean or Date value types.");
			}
		} catch (SQLException e) {
			throw new CelestaCritical(e.getMessage());
		}
	}

}

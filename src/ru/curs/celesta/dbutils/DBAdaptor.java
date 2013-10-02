package ru.curs.celesta.dbutils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import ru.curs.celesta.AppSettings;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.Index;
import ru.curs.celesta.score.Table;

/**
 * Адаптер соединения с БД, выполняющий команды, необходимые системе обновления.
 * 
 */
public abstract class DBAdaptor {
	/*
	 * NB для программистов. Класс большой, во избежание хаоса здесь порядок
	 * такой: прежде всего -- метод getAdaptor(), далее идут public final
	 * методы, далее --- внутренняя кухня (default final и default static
	 * методы), в самом низу -- все объявления абстрактных методов.
	 */

	/**
	 * Фабрика классов адаптеров подходящего под текущие настройки типа.
	 * 
	 * @throws CelestaException
	 *             При ошибке создания адаптера (например, при создании адаптера
	 *             не поддерживаемого типа).
	 */
	public static DBAdaptor getAdaptor() throws CelestaException {
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
			throw new CelestaException("Unknown or unsupported database type.");
		}
	}

	/**
	 * Проверка на валидность соединения.
	 * 
	 * @param conn
	 *            соединение.
	 * @param timeout
	 *            тайм-аут.
	 * @return true если соединение валидно, иначе false
	 * @throws CelestaException
	 *             при возникновении ошибки работы с БД.
	 */
	public boolean isValidConnection(Connection conn, int timeout)
			throws CelestaException {
		try {
			return conn.isValid(timeout);
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
	}

	/**
	 * Получить шаблон имени таблицы.
	 */
	public String tableTemplate() {
		return "%s.%s";
	}

	/**
	 * Удалить таблицу.
	 * 
	 * @param t
	 *            удаляемая таблица
	 * @throws CelestaException
	 *             в случае ошибки работы с БД
	 */
	public void dropTables(Table t) throws CelestaException {
		Connection conn = ConnectionPool.get();
		try {
			PreparedStatement pstmt = conn.prepareStatement(String.format(
					"DROP TABLE " + tableTemplate(), t.getGrain().getName(),
					t.getName()));
			pstmt.executeQuery();
			postDropTable(conn, t);
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		} finally {
			ConnectionPool.putBack(conn);
		}
	}

	/**
	 * Вызывается после удаления таблицы.
	 * 
	 * @param conn
	 *            соединение
	 * @param table
	 *            таблица
	 * @throws CelestaException
	 *             при возникновении ошибки
	 */
	public void postDropTable(Connection conn, Table table)
			throws CelestaException {

	}

	/**
	 * Возвращает true в том и только том случае, если база данных содержит
	 * таблицу.
	 * 
	 * @param schema
	 *            схема.
	 * @param name
	 *            имя таблицы.
	 * @throws CelestaException
	 *             ошибка БД
	 */
	public final boolean tableExists(String schema, String name)
			throws CelestaException {
		Connection conn = ConnectionPool.get();
		try {
			return tableExists(conn, schema, name);
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		} finally {
			ConnectionPool.putBack(conn);
		}
	}

	/**
	 * Возвращает true в том и только том случае, если база данных содержит
	 * пользовательские таблицы (т. е. не является пустой базой данных).
	 * 
	 * @throws CelestaException
	 *             ошибка БД
	 */
	public final boolean userTablesExist() throws CelestaException {
		Connection conn = ConnectionPool.get();
		try {
			return userTablesExist(conn);
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		} finally {
			ConnectionPool.putBack(conn);
		}
	}

	/**
	 * Создаёт в базе данных схему с указанным именем, если таковая схема ранее
	 * не существовала.
	 * 
	 * @param name
	 *            имя схемы.
	 * @throws CelestaException
	 *             только в том случае, если возник критический сбой при
	 *             создании схемы. Не выбрасывается в случае, если схема с
	 *             данным именем уже существует в базе данных.
	 */
	public final void createSchemaIfNotExists(String name)
			throws CelestaException {
		Connection conn = ConnectionPool.get();
		try {
			createSchemaIfNotExists(conn, name);
		} catch (SQLException e) {
			throw new CelestaException("Cannot create schema. "
					+ e.getMessage());
		} finally {
			ConnectionPool.putBack(conn);
		}
	}

	/**
	 * Создаёт в базе данных таблицу "с нуля".
	 * 
	 * @param table
	 *            Таблица для создания.
	 * @throws CelestaException
	 *             В случае возникновения критического сбоя при создании
	 *             таблицы, в том числе в случае, если такая таблица существует.
	 */
	public final void createTable(Table table) throws CelestaException {
		String def = tableDef(table);
		Connection conn = ConnectionPool.get();
		try {
			PreparedStatement stmt = conn.prepareStatement(def);
			stmt.execute();
			stmt.close();
			postCreateTable(conn, table);
		} catch (SQLException e) {
			throw new CelestaException("Cannot create table. " + e.getMessage());
		} finally {
			ConnectionPool.putBack(conn);
		}
	}

	/**
	 * Вызывается после создания таблицы.
	 * 
	 * @param conn
	 *            соединение
	 * @param table
	 *            таблица
	 * @throws CelestaException
	 *             при возникновении ошибки
	 */
	public void postCreateTable(Connection conn, Table table)
			throws CelestaException {

	}

	/**
	 * Добавляет к таблице новую колонку.
	 * 
	 * @param conn
	 *            Соединение с БД.
	 * 
	 * @param c
	 *            Колонка для добавления.
	 * @throws CelestaException
	 *             при ошибке добавления колонки.
	 */
	public final void createColumn(Connection conn, Column c)
			throws CelestaException {
		String sql = String.format(
				"alter table " + tableTemplate() + " add %s", c
						.getParentTable().getGrain().getName(), c
						.getParentTable().getName(), columnDef(c));
		try {
			Statement stmt = conn.createStatement();
			try {
				stmt.executeUpdate(sql);
			} finally {
				stmt.close();
			}
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
	}

	/**
	 * Возвращает набор имён индексов, связанных с таблицами, лежащими в
	 * указанной грануле.
	 * 
	 * @param conn
	 *            Соединение с БД.
	 * @param g
	 *            Гранула, по таблицам которой следует просматривать индексы.
	 * @throws CelestaException
	 *             В случае сбоя связи с БД.
	 */
	public Set<String> getIndices(Connection conn, Grain g)
			throws CelestaException {
		Set<String> result = new HashSet<String>();
		try {
			for (Table t : g.getTables().values()) {
				DatabaseMetaData metaData = conn.getMetaData();
				ResultSet rs = metaData.getIndexInfo(null, t.getGrain()
						.getName(), t.getName(), false, false);
				try {
					while (rs.next()) {
						String rIndexName = rs.getString("INDEX_NAME");
						if (rIndexName != null) {
							result.add(rIndexName);
						}
					}
				} finally {
					rs.close();
				}
			}
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
		return result;
	}

	/**
	 * Возвращает набор имён столбцов определённой таблицы.
	 * 
	 * @param conn
	 *            Соединение с БД.
	 * @param t
	 *            Таблица, по которой просматривать столбцы.
	 * 
	 * @throws CelestaException
	 *             в случае сбоя связи с БД.
	 */
	public Set<String> getColumns(Connection conn, Table t)
			throws CelestaException {
		Set<String> result = new HashSet<String>();
		try {
			DatabaseMetaData metaData = conn.getMetaData();
			ResultSet rs = metaData.getColumns(null, t.getGrain().getName(),
					t.getName(), null);
			try {
				while (rs.next()) {
					String rColumnName = rs.getString("COLUMN_NAME");
					result.add(rColumnName);
				}
			} finally {
				rs.close();
			}
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
		return result;
	}

	/**
	 * Создаёт в грануле индекс на таблице.
	 * 
	 * @param index
	 *            описание индекса.
	 * @throws CelestaException
	 *             Если что-то пошло не так.
	 */
	public final void createIndex(Index index) throws CelestaException {
		// TODO
	}

	/**
	 * Удаляет в грануле индекс на таблице.
	 * 
	 * @param g
	 *            Гранула
	 * @param indexName
	 *            Имя индекса
	 */
	public final void dropIndex(Grain g, String indexName) {
		// TODO Auto-generated method stub
	}

	/**
	 * Возвращает наименование типа столбца, соответствующее базе данных.
	 * 
	 * @param c
	 *            Колонка в score
	 */
	final String dbFieldType(Column c) {
		return getColumnDefiner(c).dbFieldType();
	}

	final String columnDef(Column c) {
		return getColumnDefiner(c).getColumnDef(c);
	}

	final String tableDef(Table table) {
		StringBuilder sb = new StringBuilder();
		// Определение таблицы с колонками
		sb.append(String.format("create table " + tableTemplate() + "(\n",
				table.getGrain().getName(), table.getName()));
		boolean multiple = false;
		for (Column c : table.getColumns().values()) {
			if (multiple)
				sb.append(",\n");
			sb.append("  " + columnDef(c));
			multiple = true;
		}
		sb.append(",\n");
		// Определение первичного ключа (он у нас всегда присутствует)
		sb.append(String.format("  constraint %s primary key (", table
				.getPkConstraintName() == null ? "pk_" + table.getName()
				: table.getPkConstraintName()));
		multiple = false;
		for (String s : table.getPrimaryKey().keySet()) {
			if (multiple)
				sb.append(", ");
			sb.append(s);
			multiple = true;
		}
		sb.append(")\n)");
		return sb.toString();
	}

	static PreparedStatement prepareStatement(Connection conn, String sql)
			throws CelestaException {
		try {
			return conn.prepareStatement(sql);
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
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

	final String getSelectFromOrderBy(Table t, String whereClause,
			List<String> orderBy) {
		String sqlfrom = String.format("select %s from " + tableTemplate(),
				getTableFieldsList(t), t.getGrain().getName(), t.getName());

		String sqlwhere = "".equals(whereClause) ? "" : " where " + whereClause;

		String orderByList = getFieldList(orderBy);
		String sqlorder = "".equals(orderByList) ? "" : " order by "
				+ orderByList;

		return sqlfrom + sqlwhere + sqlorder;
	}

	static String getRecordWhereClause(Table t) {
		StringBuilder whereClause = new StringBuilder();
		for (String fieldName : t.getPrimaryKey().keySet())
			whereClause.append(String.format("%s(%s = ?)",
					whereClause.length() > 0 ? " and " : "", fieldName));
		return whereClause.toString();
	}

	static void setParam(PreparedStatement stmt, int i, Object v)
			throws CelestaException {
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
				throw new CelestaException(
						"You can filter only on Integer, Double, String, "
								+ "Boolean or Date value types.");
			}
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
	}

	static Set<String> sqlToStringSet(Connection conn, String sql)
			throws CelestaException {
		Set<String> result = new HashSet<String>();
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			try {
				while (rs.next()) {
					result.add(rs.getString(1));
				}
			} finally {
				stmt.close();
			}
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
		return result;
	}

	abstract ColumnDefiner getColumnDefiner(Column c);

	abstract boolean tableExists(Connection conn, String schema, String name)
			throws SQLException;

	abstract boolean userTablesExist(Connection conn) throws SQLException;

	abstract void createSchemaIfNotExists(Connection conn, String name)
			throws SQLException;

	abstract PreparedStatement getOneRecordStatement(Connection conn, Table t)
			throws CelestaException;

	abstract PreparedStatement getRecordSetStatement(Connection conn, Table t,
			Map<String, AbstractFilter> filters, List<String> orderBy)
			throws CelestaException;

	abstract PreparedStatement deleteRecordSetStatement(Connection conn,
			Table t, Map<String, AbstractFilter> filters)
			throws CelestaException;

	abstract PreparedStatement getInsertRecordStatement(Connection conn,
			Table t, boolean[] nullsMask) throws CelestaException;

	abstract int getCurrentIdent(Connection conn, Table t) throws CelestaException;
	
	abstract PreparedStatement getUpdateRecordStatement(Connection conn, Table t)
			throws CelestaException;

	abstract PreparedStatement getDeleteRecordStatement(Connection conn, Table t)
			throws CelestaException;

}

/**
 * Класс, ответственный за генерацию определения столбца таблицы в разных СУБД.
 * 
 */
abstract class ColumnDefiner {
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

package ru.curs.celesta.dbutils;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import ru.curs.celesta.AppSettings;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.score.BinaryColumn;
import ru.curs.celesta.score.BooleanColumn;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.DateTimeColumn;
import ru.curs.celesta.score.FloatingColumn;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.Index;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.StringColumn;
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
	static final String NOT_IMPLEMENTED_YET = "not implemented yet";

	static final Class<?>[] COLUMN_CLASSES = { IntegerColumn.class,
			StringColumn.class, BooleanColumn.class, FloatingColumn.class,
			BinaryColumn.class, DateTimeColumn.class };
	static final String COLUMN_NAME = "COLUMN_NAME";

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
		return "\"%s\".\"%s\"";
	}

	/**
	 * Удалить таблицу.
	 * 
	 * @param t
	 *            удаляемая таблица
	 * @throws CelestaException
	 *             в случае ошибки работы с БД
	 */
	public void dropTable(Table t) throws CelestaException {
		Connection conn = ConnectionPool.get();
		try {
			String sql = String.format("DROP TABLE " + tableTemplate(), t
					.getGrain().getName(), t.getName());
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.execute();
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
			// System.out.println(def); // for debug purposes
			PreparedStatement stmt = conn.prepareStatement(def);
			stmt.execute();
			stmt.close();
			postCreateTable(conn, table);
		} catch (SQLException e) {
			throw new CelestaException("creating %s: %s", table.getName(),
					e.getMessage());
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
			throw new CelestaException("creating %s.%s: %s", c.getParentTable()
					.getName(), c.getName(), e.getMessage());
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
	public Map<DBIndexInfo, TreeMap<Short, String>> getIndices(Connection conn,
			Grain g) throws CelestaException {
		Map<DBIndexInfo, TreeMap<Short, String>> result = new HashMap<>();
		try {
			for (Table t : g.getTables().values()) {
				DatabaseMetaData metaData = conn.getMetaData();
				ResultSet rs = metaData.getIndexInfo(null, t.getGrain()
						.getName(), t.getName(), false, false);
				try {
					while (rs.next()) {
						String indName = rs.getString("INDEX_NAME");
						if (indName != null && rs.getBoolean("NON_UNIQUE")) {
							DBIndexInfo info = new DBIndexInfo(t.getName(),
									indName);
							TreeMap<Short, String> columns = result.get(info);
							if (columns == null) {
								columns = new TreeMap<>();
								result.put(info, columns);
							}
							columns.put(rs.getShort("ORDINAL_POSITION"),
									rs.getString(COLUMN_NAME));
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
		Set<String> result = new LinkedHashSet<>();
		try {
			DatabaseMetaData metaData = conn.getMetaData();
			ResultSet rs = metaData.getColumns(null, t.getGrain().getName(),
					t.getName(), null);
			try {
				while (rs.next()) {
					String rColumnName = rs.getString(COLUMN_NAME);
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
	 * Возвращает условие where на таблице, исходя из текущих фильтров.
	 * 
	 * @param filters
	 *            фильтры
	 */
	final String getWhereClause(Map<String, AbstractFilter> filters) {
		if (filters == null)
			throw new IllegalArgumentException();
		StringBuilder whereClause = new StringBuilder();
		for (Entry<String, AbstractFilter> e : filters.entrySet()) {
			if (whereClause.length() > 0)
				whereClause.append(" and ");
			if (e.getValue() instanceof SingleValue)
				whereClause.append(String.format("(\"%s\" = ?)", e.getKey()));
			else if (e.getValue() instanceof Range)
				whereClause.append(String.format("(\"%s\" between ? and ?)",
						e.getKey()));
			else if (e.getValue() instanceof Filter)
				throw new RuntimeException(NOT_IMPLEMENTED_YET);
		}
		return whereClause.toString();
	}

	/**
	 * Устанавливает параметры на запрос по фильтрам.
	 * 
	 * @param filters
	 *            Фильтры, с которыми вызывался getWhereClause
	 * @throws CelestaException
	 *             в случае сбоя JDBC
	 */
	final void fillSetQueryParameters(Map<String, AbstractFilter> filters,
			PreparedStatement result) throws CelestaException {
		int i = 1;
		for (AbstractFilter f : filters.values()) {
			if (f instanceof SingleValue) {
				setParam(result, i++, ((SingleValue) f).getValue());
			} else if (f instanceof Range) {
				setParam(result, i++, ((Range) f).getValueFrom());
				setParam(result, i++, ((Range) f).getValueTo());
			} else if (f instanceof Filter)
				throw new RuntimeException(NOT_IMPLEMENTED_YET);
		}
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
		String sql = getCreateIndexSQL(index);
		Connection conn = ConnectionPool.get();
		try {
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.execute();
			stmt.close();
		} catch (SQLException e) {
			throw new CelestaException("Cannot create index '%s': %s",
					index.getName(), e.getMessage());
		} finally {
			ConnectionPool.putBack(conn);
		}
	}

	/**
	 * Удаляет в грануле индекс на таблице.
	 * 
	 * @param g
	 *            Гранула
	 * @param dBIndexInfo
	 *            Массив из двух элементов: имя таблицы, имя индекса
	 * @throws CelestaException
	 *             Если что-то пошло не так.
	 */
	public final void dropIndex(Grain g, DBIndexInfo dBIndexInfo)
			throws CelestaException {
		String sql = getDropIndexSQL(g, dBIndexInfo);
		Connection conn = ConnectionPool.get();
		try {
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.execute();
			stmt.close();
		} catch (SQLException e) {
			throw new CelestaException("Cannot drop index '%s': %s ",
					dBIndexInfo.getIndexName(), e.getMessage());
		} finally {
			ConnectionPool.putBack(conn);
		}
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
		return getColumnDefiner(c).getFullDefinition(c);
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
		sb.append(String.format("  constraint \"%s\" primary key (", table
				.getPkConstraintName() == null ? "pk_" + table.getName()
				: table.getPkConstraintName()));
		multiple = false;
		for (String s : table.getPrimaryKey().keySet()) {
			if (multiple)
				sb.append(", ");
			sb.append('"');
			sb.append(s);
			sb.append('"');
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
			sb.append('"');
			sb.append(c);
			sb.append('"');
		}
		return sb.toString();
	}

	static String getTableFieldsListExceptBLOBs(Table t) {
		List<String> flds = new LinkedList<String>();
		for (Map.Entry<String, Column> e : t.getColumns().entrySet()) {
			if (!(e.getValue() instanceof BinaryColumn))
				flds.add(e.getKey());
		}
		return getFieldList(flds);
	}

	final String getSelectFromOrderBy(Table t, String whereClause,
			List<String> orderBy) {
		String sqlfrom = String.format("select %s from " + tableTemplate(),
				getTableFieldsListExceptBLOBs(t), t.getGrain().getName(),
				t.getName());

		String sqlwhere = "".equals(whereClause) ? "" : " where " + whereClause;

		String orderByList = getFieldList(orderBy);
		String sqlorder = "".equals(orderByList) ? "" : " order by "
				+ orderByList;

		return sqlfrom + sqlwhere + sqlorder;
	}

	static String getRecordWhereClause(Table t) {
		StringBuilder whereClause = new StringBuilder();
		for (String fieldName : t.getPrimaryKey().keySet())
			whereClause.append(String.format("%s(\"%s\" = ?)",
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
			} else if (v instanceof BLOB) {
				Blob b = stmt.getConnection().createBlob();
				((BLOB) v).saveToJDBCBlob(b);
				stmt.setBlob(i, b);
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

	final PreparedStatement getSetCountStatement(Connection conn, Table t,
			Map<String, AbstractFilter> filters) throws CelestaException {
		String whereClause = getWhereClause(filters);
		String sql = String.format("select count(*) from " + tableTemplate()
				+ ("".equals(whereClause) ? "" : " where " + whereClause), t
				.getGrain().getName(), t.getName());
		PreparedStatement result = prepareStatement(conn, sql);
		fillSetQueryParameters(filters, result);
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

	abstract PreparedStatement getOneFieldStatement(Connection conn, Column c)
			throws CelestaException;

	abstract PreparedStatement getRecordSetStatement(Connection conn, Table t,
			Map<String, AbstractFilter> filters, List<String> orderBy)
			throws CelestaException;

	abstract PreparedStatement deleteRecordSetStatement(Connection conn,
			Table t, Map<String, AbstractFilter> filters)
			throws CelestaException;

	abstract PreparedStatement getInsertRecordStatement(Connection conn,
			Table t, boolean[] nullsMask) throws CelestaException;

	abstract int getCurrentIdent(Connection conn, Table t)
			throws CelestaException;

	abstract PreparedStatement getUpdateRecordStatement(Connection conn,
			Table t, boolean[] equalsMask) throws CelestaException;

	abstract PreparedStatement getDeleteRecordStatement(Connection conn, Table t)
			throws CelestaException;

	abstract String getCreateIndexSQL(Index index);

	abstract String getDropIndexSQL(Grain g, DBIndexInfo dBIndexInfo);

	/**
	 * Возвращает информацию о столбце.
	 * 
	 * @param conn
	 *            Соединение с БД.
	 * 
	 * @param c
	 *            Столбец.
	 * @throws CelestaException
	 *             в случае сбоя связи с БД.
	 */
	abstract DBColumnInfo getColumnInfo(Connection conn, Column c)
			throws CelestaException;

	/**
	 * Обновляет на таблице колонку.
	 * 
	 * @param conn
	 *            Соединение с БД.
	 * 
	 * @param c
	 *            Колонка для обновления.
	 * @throws CelestaException
	 *             при ошибке обновления колонки.
	 */
	abstract void updateColumn(Connection conn, Column c, DBColumnInfo actual)
			throws CelestaException;

}

/**
 * Класс, ответственный за генерацию определения столбца таблицы в разных СУБД.
 * 
 */
abstract class ColumnDefiner {
	static final String DEFAULT = "default ";

	abstract String dbFieldType();

	/**
	 * Возвращает определение колонки, содержащее имя, тип и NULL/NOT NULL (без
	 * DEFAULT). Требуется для механизма изменения колонок.
	 * 
	 * @param c
	 *            колонка.
	 */
	abstract String getMainDefinition(Column c);

	/**
	 * Отдельно возвращает DEFAULT-определение колонки.
	 * 
	 * @param c
	 *            колонка.
	 */
	abstract String getDefaultDefinition(Column c);

	/**
	 * Возвращает полное определение колонки (для создания колонки).
	 * 
	 * @param c
	 *            колонка
	 */
	String getFullDefinition(Column c) {
		return join(getMainDefinition(c), getDefaultDefinition(c));
	}

	String nullable(Column c) {
		return c.isNullable() ? "null" : "not null";
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
 * Данные о колонке в базе данных в виде, необходимом для Celesta.
 */
final class DBColumnInfo {
	private String name;
	private Class<? extends Column> type;
	private boolean isNullable;
	private String defaultValue = "";
	private int length;
	private boolean isMax;
	private boolean isIdentity;

	String getName() {
		return name;
	}

	Class<? extends Column> getType() {
		return type;
	}

	boolean isNullable() {
		return isNullable;
	}

	String getDefaultValue() {
		return defaultValue;
	}

	int getLength() {
		return length;
	}

	boolean isMax() {
		return isMax;
	}

	boolean isIdentity() {
		return isIdentity;
	}

	void setName(String name) {
		this.name = name;
	}

	void setType(Class<? extends Column> type) {
		this.type = type;
	}

	void setNullable(boolean isNullable) {
		this.isNullable = isNullable;
	}

	void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}

	void setLength(int length) {
		this.length = length;
	}

	void setMax(boolean isMax) {
		this.isMax = isMax;
	}

	void setIdentity(boolean isIdentity) {
		this.isIdentity = isIdentity;
	}

	boolean reflects(Column value) {
		// Если тип не совпадает -- дальше не проверяем.
		if (value.getClass() != type)
			return false;

		// Проверяем nullability, но помним о том, что в Oracle DEFAULT
		// ''-строки всегда nullable
		if (type != StringColumn.class || !"''".equals(defaultValue)) {
			if (value.isNullable() != isNullable)
				return false;
		}

		if (type == IntegerColumn.class) {
			// Если свойство IDENTITY не совпадает -- не проверяем
			if (isIdentity != ((IntegerColumn) value).isIdentity())
				return false;
		} else if (type == StringColumn.class) {
			// Если параметры длин не совпали -- не проверяем
			StringColumn col = (StringColumn) value;
			if (!(isMax ? col.isMax() : length == col.getLength()))
				return false;
		}

		// Если в данных пустой default, а в метаданных -- не пустой -- то
		// не проверяем
		if (defaultValue.isEmpty())
			return value.getDefaultValue() == null;

		// Случай непустого default-значения в данных.
		return checkDefault(value);
	}

	private boolean checkDefault(Column value) {
		boolean result;
		if (type == BooleanColumn.class) {
			try {
				result = BooleanColumn.parseSQLBool(defaultValue).equals(
						value.getDefaultValue());
			} catch (ParseException e) {
				result = false;
			}
		} else if (type == IntegerColumn.class) {
			result = Integer.valueOf(defaultValue).equals(
					value.getDefaultValue());
		} else if (type == FloatingColumn.class) {
			result = Double.valueOf(defaultValue).equals(
					value.getDefaultValue());
		} else if (type == DateTimeColumn.class) {
			if ("GETDATE()".equalsIgnoreCase(defaultValue))
				result = ((DateTimeColumn) value).isGetdate();
			else {
				try {
					result = DateTimeColumn.parseISODate(defaultValue).equals(
							value.getDefaultValue());
				} catch (ParseException e) {
					result = false;
				}
			}
		} else {
			result = defaultValue.equals(value.getDefaultValue());
		}
		return result;
	}
}

/**
 * Информация об индексе, полученная из метаданых базы данных.
 */
final class DBIndexInfo {
	private final String tableName;
	private final String indexName;
	private final int hash;

	DBIndexInfo(String tableName, String indexName) {
		this.tableName = tableName;
		this.indexName = indexName;
		hash = Integer.rotateLeft(tableName.hashCode(), 3)
				^ indexName.hashCode();
	}

	String getTableName() {
		return tableName;
	}

	String getIndexName() {
		return indexName;
	}

	@Override
	public int hashCode() {
		return hash;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof DBIndexInfo) {
			DBIndexInfo ii = (DBIndexInfo) obj;
			return tableName.equals(ii.tableName)
					&& indexName.equals(ii.indexName);
		}
		return super.equals(obj);
	}

	@Override
	public String toString() {
		return String.format("%s.%s", tableName, indexName);
	}

}

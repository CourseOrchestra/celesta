package ru.curs.celesta.dbutils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import ru.curs.celesta.AppSettings;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.score.BinaryColumn;
import ru.curs.celesta.score.BooleanColumn;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.DateTimeColumn;
import ru.curs.celesta.score.Expr;
import ru.curs.celesta.score.FKRule;
import ru.curs.celesta.score.FloatingColumn;
import ru.curs.celesta.score.ForeignKey;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.GrainElement;
import ru.curs.celesta.score.Index;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.SQLGenerator;
import ru.curs.celesta.score.StringColumn;
import ru.curs.celesta.score.Table;
import ru.curs.celesta.score.View;
import ru.curs.celesta.score.ViewColumnType;

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
	static final String ALTER_TABLE = "alter table ";
	static final Pattern HEXSTR = Pattern
			.compile("0x(([0-9A-Fa-f][0-9A-Fa-f])+)");

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
	 * @param conn
	 *            Соединение с БД
	 * @param t
	 *            удаляемая таблица
	 * @throws CelestaException
	 *             в случае ошибки работы с БД
	 */
	public final void dropTable(Connection conn, Table t)
			throws CelestaException {
		try {
			String sql = String.format("DROP TABLE " + tableTemplate(), t
					.getGrain().getName(), t.getName());
			Statement stmt = conn.createStatement();
			try {
				stmt.executeUpdate(sql);
			} finally {
				stmt.close();
			}
			dropAutoIncrement(conn, t);
			conn.commit();
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
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
	 * @param conn
	 *            Соединение.
	 * @param table
	 *            Таблица для создания.
	 * @throws CelestaException
	 *             В случае возникновения критического сбоя при создании
	 *             таблицы, в том числе в случае, если такая таблица существует.
	 */
	public final void createTable(Connection conn, Table table)
			throws CelestaException {
		String def = tableDef(table);
		try {
			// System.out.println(def); // for debug purposes
			Statement stmt = conn.createStatement();
			try {
				stmt.executeUpdate(def);
			} finally {
				stmt.close();
			}
			manageAutoIncrement(conn, table);
			ConnectionPool.commit(conn);
			updateVersioningTrigger(conn, table);
		} catch (SQLException e) {
			throw new CelestaException("creating %s: %s", table.getName(),
					e.getMessage());
		}
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
		String sql = String.format(ALTER_TABLE + tableTemplate() + " add %s", c
				.getParentTable().getGrain().getName(), c.getParentTable()
				.getName(), columnDef(c));
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

	final PreparedStatement getUpdateRecordStatement(Connection conn, Table t,
			boolean[] equalsMask) throws CelestaException {
		StringBuilder setClause = new StringBuilder();
		if (t.isVersioned())
			setClause.append(String.format("\"%s\" = ?", Table.RECVERSION));

		int i = 0;
		for (String c : t.getColumns().keySet()) {
			// Пропускаем ключевые поля и поля, не изменившие своего значения
			if (!(equalsMask[i] || t.getPrimaryKey().containsKey(c))) {
				padComma(setClause);
				setClause.append(String.format("\"%s\" = ?", c));
			}
			i++;
		}

		String sql = String.format("update " + tableTemplate()
				+ " set %s where %s", t.getGrain().getName(), t.getName(),
				setClause.toString(), getRecordWhereClause(t));
		return prepareStatement(conn, sql);
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
	 * @throws CelestaException
	 *             в случае некорректного фильтра
	 */
	final String getWhereClause(GrainElement t,
			Map<String, AbstractFilter> filters, Expr complexFilter)
			throws CelestaException {
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
			else if (e.getValue() instanceof Filter) {
				Object c = t.getColumns().get(e.getKey());
				whereClause.append("(");
				whereClause.append(((Filter) e.getValue()).makeWhereClause("\""
						+ e.getKey() + "\"", c));
				whereClause.append(")");
			}
		}
		if (complexFilter != null) {
			if (whereClause.length() > 0)
				whereClause.append(" and ");
			whereClause.append("(");
			whereClause.append(complexFilter.getSQL(this));
			whereClause.append(")");
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
			}
			// Пока что фильтры параметров не требуют
			// else if (f instanceof Filter)
			// throw new RuntimeException(NOT_IMPLEMENTED_YET);
		}
	}

	/**
	 * Создаёт в грануле индекс на таблице.
	 * 
	 * @param conn
	 *            Соединение с БД.
	 * 
	 * @param index
	 *            описание индекса.
	 * @throws CelestaException
	 *             Если что-то пошло не так.
	 */
	public final void createIndex(Connection conn, Index index)
			throws CelestaException {
		String sql = getCreateIndexSQL(index);
		try {
			Statement stmt = conn.createStatement();
			try {
				stmt.executeUpdate(sql);
			} finally {
				stmt.close();
			}
			ConnectionPool.commit(conn);
		} catch (SQLException e) {
			throw new CelestaException("Cannot create index '%s': %s",
					index.getName(), e.getMessage());
		}
	}

	/**
	 * Создаёт первичный ключ.
	 * 
	 * @param conn
	 *            соединение с БД.
	 * @param fk
	 *            первичный ключ
	 * @throws CelestaException
	 *             в случае неудачи создания ключа
	 */
	public final void createFK(Connection conn, ForeignKey fk)
			throws CelestaException {
		LinkedList<StringBuilder> sqlQueue = new LinkedList<>();

		// Строим запрос на создание FK
		StringBuilder sql = new StringBuilder();
		sql.append(ALTER_TABLE);
		sql.append(String.format(tableTemplate(), fk.getParentTable()
				.getGrain().getName(), fk.getParentTable().getName()));
		sql.append(" add constraint \"");
		sql.append(fk.getConstraintName());
		sql.append("\" foreign key (");
		boolean needComma = false;
		for (String name : fk.getColumns().keySet()) {
			if (needComma)
				sql.append(", ");
			sql.append('"');
			sql.append(name);
			sql.append('"');
			needComma = true;
		}
		sql.append(") references ");
		sql.append(String.format(tableTemplate(), fk.getReferencedTable()
				.getGrain().getName(), fk.getReferencedTable().getName()));
		sql.append("(");
		needComma = false;
		for (String name : fk.getReferencedTable().getPrimaryKey().keySet()) {
			if (needComma)
				sql.append(", ");
			sql.append('"');
			sql.append(name);
			sql.append('"');
			needComma = true;
		}
		sql.append(")");

		switch (fk.getDeleteRule()) {
		case SET_NULL:
			sql.append(" on delete set null");
			break;
		case CASCADE:
			sql.append(" on delete cascade");
			break;
		case NO_ACTION:
		default:
			break;
		}

		sqlQueue.add(sql);
		processCreateUpdateRule(fk, sqlQueue);

		// Построили, выполняем
		for (StringBuilder sqlStmt : sqlQueue) {
			String sqlstmt = sqlStmt.toString();

			// System.out.println("----------------");
			// System.out.println(sqlStmt);

			try {
				Statement stmt = conn.createStatement();
				try {
					stmt.executeUpdate(sqlstmt);
				} finally {
					stmt.close();
				}
			} catch (SQLException e) {
				if (!sqlstmt.startsWith("drop"))
					throw new CelestaException(
							"Cannot create foreign key '%s': %s",
							fk.getConstraintName(), e.getMessage());
			}
		}
	}

	/**
	 * Удаляет первичный ключ из базы данных.
	 * 
	 * @param conn
	 *            Соединение с БД
	 * @param grainName
	 *            имя гранулы
	 * @param tableName
	 *            Имя таблицы, на которой определён первичный ключ.
	 * @param fkName
	 *            Имя первичного ключа.
	 * @throws CelestaException
	 *             В случае сбоя в базе данных.
	 */
	public void dropFK(Connection conn, String grainName, String tableName,
			String fkName) throws CelestaException {
		LinkedList<String> sqlQueue = new LinkedList<>();
		String sql = String.format("alter table " + tableTemplate()
				+ " drop constraint \"%s\"", grainName, tableName, fkName);
		sqlQueue.add(sql);
		processDropUpdateRule(sqlQueue, fkName);
		// Построили, выполняем
		for (String sqlStmt : sqlQueue) {
			// System.out.println(sqlStmt);
			try {
				Statement stmt = conn.createStatement();
				try {
					stmt.executeUpdate(sqlStmt);
				} finally {
					stmt.close();
				}
			} catch (SQLException e) {
				if (!sqlStmt.startsWith("drop trigger"))
					throw new CelestaException(
							"Cannot drop foreign key '%s': %s", fkName,
							e.getMessage());
			}
		}
	}

	void processDropUpdateRule(LinkedList<String> sqlQueue, String fkName) {

	}

	void processCreateUpdateRule(ForeignKey fk, LinkedList<StringBuilder> queue) {
		StringBuilder sql = queue.peek();
		switch (fk.getUpdateRule()) {
		case SET_NULL:
			sql.append(" on update set null");
			break;
		case CASCADE:
			sql.append(" on update cascade");
			break;
		case NO_ACTION:
		default:
			break;
		}
	}

	/**
	 * Удаляет в грануле индекс на таблице.
	 * 
	 * @param g
	 *            Гранула
	 * @param dBIndexInfo
	 *            Информация об индексе
	 * @param suppressError
	 *            Гасить ошибку удаления индекса (актуально для MySQL, в которой
	 *            не все индексы могут быть удалены прежде внешнего ключа).
	 * @throws CelestaException
	 *             Если что-то пошло не так.
	 */
	public final void dropIndex(Grain g, DBIndexInfo dBIndexInfo,
			boolean suppressError) throws CelestaException {
		String sql = getDropIndexSQL(g, dBIndexInfo);
		Connection conn = ConnectionPool.get();
		try {
			Statement stmt = conn.createStatement();
			stmt.executeUpdate(sql);
			stmt.close();
		} catch (SQLException e) {
			if (!suppressError)
				throw new CelestaException("Cannot drop index '%s': %s ",
						dBIndexInfo.getIndexName(), e.getMessage());
		} finally {
			ConnectionPool.putBack(conn);
		}
	}

	/**
	 * Возвращает PreparedStatement, содержащий отфильтрованный набор записей.
	 * 
	 * @param conn
	 *            Соединение.
	 * @param t
	 *            Таблица.
	 * @param filters
	 *            Фильтры на таблице.
	 * @param orderBy
	 *            Порядок сортировки.
	 * @param offset
	 *            Количество строк для пропуска
	 * @param rowCount
	 *            Количество строк для возврата (limit-фильтр).
	 * @throws CelestaException
	 *             Ошибка БД или некорректный фильтр.
	 */
	// CHECKSTYLE:OFF 6 parameters
	public final PreparedStatement getRecordSetStatement(Connection conn,
			GrainElement t, Map<String, AbstractFilter> filters,
			Expr complexFilter, String orderBy, long offset, long rowCount)
			throws CelestaException {
		// CHECKSTYLE:ON
		String sql;
		// Готовим условие where
		String whereClause = getWhereClause(t, filters, complexFilter);

		if (offset == 0 && rowCount == 0) {
			// Запрос не лимитированный -- одинаков для всех СУБД
			// Соединяем полученные компоненты в стандартный запрос
			// SELECT..FROM..WHERE..ORDER BY
			sql = getSelectFromOrderBy(t, whereClause, orderBy);
		} else {
			sql = getLimitedSQL(t, whereClause, orderBy, offset, rowCount);

			System.out.println(sql);
		}
		try {
			PreparedStatement result = conn.prepareStatement(sql);
			// А теперь заполняем параметры
			fillSetQueryParameters(filters, result);
			return result;
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
	}

	abstract String getLimitedSQL(GrainElement t, String whereClause,
			String orderBy, long offset, long rowCount);

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
		// У версионированных таблиц - колонка recversion
		if (table.isVersioned())
			sb.append("  " + columnDef(table.getRecVersionField()) + ",\n");

		// Определение первичного ключа (он у нас всегда присутствует)
		sb.append(String.format("  constraint \"%s\" primary key (",
				table.getPkConstraintName()));
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

	final String getSelectFromOrderBy(GrainElement t, String whereClause,
			String orderBy) {
		String sqlfrom = String.format("select %s from " + tableTemplate(),
				getTableFieldsListExceptBLOBs(t), t.getGrain().getName(),
				t.getName());

		String sqlwhere = "".equals(whereClause) ? "" : " where " + whereClause;

		return sqlfrom + sqlwhere + " order by " + orderBy;
	}

	final PreparedStatement getSetCountStatement(Connection conn,
			GrainElement t, Map<String, AbstractFilter> filters,
			Expr complexFilter) throws CelestaException {
		String whereClause = getWhereClause(t, filters, complexFilter);
		String sql = "select count(*) from "
				+ String.format(tableTemplate(), t.getGrain().getName(),
						t.getName())
				+ ("".equals(whereClause) ? "" : " where " + whereClause);
		PreparedStatement result = prepareStatement(conn, sql);
		fillSetQueryParameters(filters, result);
		return result;
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

	static String getTableFieldsListExceptBLOBs(GrainElement t) {
		List<String> flds = new LinkedList<>();
		for (Map.Entry<String, ?> e : (t.getColumns()).entrySet()) {
			if (!(e.getValue() instanceof BinaryColumn || e.getValue() == ViewColumnType.BLOB))
				flds.add(e.getKey());
		}

		// К перечню полей версионированных таблиц обязательно добавляем
		// recversion
		if (t instanceof Table && ((Table) t).isVersioned())
			flds.add(Table.RECVERSION);

		return getFieldList(flds);
	}

	static void runUpdateColumnSQL(Connection conn, Column c, String sql)
			throws CelestaException {
		// System.out.println(sql); //for debug
		try {
			Statement stmt = conn.createStatement();
			try {
				stmt.executeUpdate(sql);
			} finally {
				stmt.close();
			}
		} catch (SQLException e) {
			throw new CelestaException(
					"Cannot modify column %s on table %s.%s: %s", c.getName(),
					c.getParentTable().getGrain().getName(), c.getParentTable()
							.getName(), e.getMessage());

		}
	}

	static FKRule getFKRule(String rule) {
		if ("NO ACTION".equalsIgnoreCase(rule)
				|| "RECTRICT".equalsIgnoreCase(rule))
			return FKRule.NO_ACTION;
		if ("SET NULL".equalsIgnoreCase(rule))
			return FKRule.SET_NULL;
		if ("CASCADE".equalsIgnoreCase(rule))
			return FKRule.CASCADE;
		return null;
	}

	static void padComma(StringBuilder insertList) {
		if (insertList.length() > 0)
			insertList.append(", ");
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
				stmt.setBinaryStream(i, ((BLOB) v).getInStream(),
						((BLOB) v).size());
				// createBlob is not implemented for PostgreSQL driver!
				// Blob b = stmt.getConnection().createBlob();
				// ((BLOB) v).saveToJDBCBlob(b);
				// stmt.setBlob(i, b);
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
			throws CelestaException;

	abstract boolean userTablesExist(Connection conn) throws SQLException;

	abstract void createSchemaIfNotExists(Connection conn, String name)
			throws SQLException;

	abstract void manageAutoIncrement(Connection conn, Table t)
			throws SQLException;

	abstract void dropAutoIncrement(Connection conn, Table t)
			throws SQLException;

	abstract PreparedStatement getOneRecordStatement(Connection conn, Table t)
			throws CelestaException;

	abstract PreparedStatement getOneFieldStatement(Connection conn, Column c)
			throws CelestaException;

	abstract PreparedStatement deleteRecordSetStatement(Connection conn,
			Table t, Map<String, AbstractFilter> filters, Expr complexFilter)
			throws CelestaException;

	abstract PreparedStatement getInsertRecordStatement(Connection conn,
			Table t, boolean[] nullsMask) throws CelestaException;

	abstract int getCurrentIdent(Connection conn, Table t)
			throws CelestaException;

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

	/**
	 * Возвращает информацию о первичном ключе таблицы.
	 * 
	 * @param conn
	 *            Соединение с БД.
	 * @param t
	 *            Таблица, информацию о первичном ключе которой необходимо
	 *            получить.
	 * @throws CelestaException
	 *             в случае сбоя связи с БД.
	 */
	abstract DBPKInfo getPKInfo(Connection conn, Table t)
			throws CelestaException;

	/**
	 * Удаляет первичный ключ на таблице с использованием известного имени
	 * первичного ключа.
	 * 
	 * @param conn
	 *            Соединение с базой данных.
	 * @param t
	 *            Таблица.
	 * @param pkName
	 *            Имя первичного ключа.
	 * @throws CelestaException
	 *             в случае сбоя связи с БД.
	 */
	abstract void dropPK(Connection conn, Table t, String pkName)
			throws CelestaException;

	/**
	 * Создаёт первичный ключ на таблице в соответствии с метаописанием.
	 * 
	 * @param conn
	 *            Соединение с базой данных.
	 * @param t
	 *            Таблица.
	 * @throws CelestaException
	 *             неудача создания первичного ключа (например, неуникальные
	 *             записи).
	 */
	abstract void createPK(Connection conn, Table t) throws CelestaException;

	abstract List<DBFKInfo> getFKInfo(Connection conn, Grain g)
			throws CelestaException;

	/**
	 * Возвращает набор индексов, связанных с таблицами, лежащими в указанной
	 * грануле.
	 * 
	 * @param conn
	 *            Соединение с БД.
	 * @param g
	 *            Гранула, по таблицам которой следует просматривать индексы.
	 * @throws CelestaException
	 *             В случае сбоя связи с БД.
	 */
	abstract Map<String, DBIndexInfo> getIndices(Connection conn, Grain g)
			throws CelestaException;

	/**
	 * Возвращает перечень имён представлений в грануле.
	 * 
	 * @param conn
	 *            Соединение с БД.
	 * @param g
	 *            Гранула, перечень имён представлений которой необходимо
	 *            получить.
	 * @throws CelestaException
	 *             В случае сбоя связи с БД.
	 */
	public List<String> getViewList(Connection conn, Grain g)
			throws CelestaException {
		String sql = String
				.format("select table_name from information_schema.views where table_schema = '%s'",
						g.getName());
		List<String> result = new LinkedList<>();
		try {
			Statement stmt = conn.createStatement();
			try {
				ResultSet rs = stmt.executeQuery(sql);
				while (rs.next()) {
					result.add(rs.getString(1));
				}
			} finally {
				stmt.close();
			}
		} catch (SQLException e) {
			throw new CelestaException("Cannot get views list: %s",
					e.toString());
		}
		return result;
	}

	/**
	 * Создаёт представление в базе данных на основе метаданных.
	 * 
	 * @param conn
	 *            Соединение с БД.
	 * @param v
	 *            Представление.
	 * @throws CelestaException
	 *             Ошибка БД.
	 */
	public void createView(Connection conn, View v) throws CelestaException {
		SQLGenerator gen = getViewSQLGenerator();
		try {
			StringWriter sw = new StringWriter();
			BufferedWriter bw = new BufferedWriter(sw);

			v.createViewScript(bw, gen);
			bw.flush();

			String sql = sw.toString();
			// System.out.println(sql);

			Statement stmt = conn.createStatement();
			try {
				stmt.executeUpdate(sql);
			} finally {
				stmt.close();
			}
		} catch (SQLException | IOException e) {
			throw new CelestaException("Error while creating view %s.%s: %s", v
					.getGrain().getName(), v.getName(), e.getMessage());

		}

	}

	/**
	 * Возвращает транслятор из языка CelestaSQL в язык нужного диалекта БД.
	 */
	public abstract SQLGenerator getViewSQLGenerator();

	/**
	 * Удаление представления.
	 * 
	 * @param conn
	 *            Соединение с БД.
	 * @param grainName
	 *            Имя гранулы.
	 * @param viewName
	 *            Имя представления.
	 * @throws CelestaException
	 *             Ошибка БД.
	 */
	public void dropView(Connection conn, String grainName, String viewName)
			throws CelestaException {
		try {
			String sql = String.format("DROP VIEW " + tableTemplate(),
					grainName, viewName);
			Statement stmt = conn.createStatement();
			try {
				stmt.executeUpdate(sql);
			} finally {
				stmt.close();
			}
			conn.commit();
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
	}

	/**
	 * Создаёт или пересоздаёт прочие системные объекты (хранимые процедуры,
	 * функции), необходимые для функционирования Celesta на текущей СУБД.
	 * 
	 * @param conn
	 *            Соединение.
	 * @throws CelestaException
	 *             Ошибка создания объектов.
	 */
	public void createSysObjects(Connection conn) throws CelestaException {

	}

	/**
	 * Обновляет триггер контроля версий на таблице.
	 * 
	 * @param conn
	 *            Соединение.
	 * @param t
	 *            Таблица (версионируемая или не версионируемая).
	 * @throws CelestaException
	 *             Ошибка создания или удаления триггера.
	 */
	public abstract void updateVersioningTrigger(Connection conn, Table t)
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
package ru.curs.celesta.dbutils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.BinaryColumn;
import ru.curs.celesta.score.BooleanColumn;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.DateTimeColumn;
import ru.curs.celesta.score.FloatingColumn;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.Index;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.StringColumn;
import ru.curs.celesta.score.Table;

/**
 * Адаптер Oracle Database.
 */
final class OraAdaptor extends DBAdaptor {
	private static final int LENGTHFORMAX = 4000;

	private static final Pattern BOOLEAN_CHECK = Pattern
			.compile("\"([^\"]+)\" *[iI][nN] *\\( *0 *, *1 *\\)");
	private static final Pattern DATE_PATTERN = Pattern
			.compile("'(\\d\\d\\d\\d)-([01]\\d)-([0123]\\d)'");
	private static final Pattern HEX_STRING = Pattern.compile("'([0-9A-F]+)'");

	private static final Map<Class<? extends Column>, OraColumnDefiner> TYPES_DICT = new HashMap<>();

	/**
	 * Определитель колонок для Oracle, учитывающий тот факт, что в Oracle
	 * DEFAULT должен идти до NOT NULL.
	 */
	abstract static class OraColumnDefiner extends ColumnDefiner {
		abstract String getInternalDefinition(Column c);

		@Override
		String getFullDefinition(Column c) {
			return join(getInternalDefinition(c), getDefaultDefinition(c),
					nullable(c));
		}

		@Override
		final String getMainDefinition(Column c) {
			return join(getInternalDefinition(c), nullable(c));
		}
	}

	static {
		TYPES_DICT.put(IntegerColumn.class, new OraColumnDefiner() {
			@Override
			String dbFieldType() {
				return "number";
			}

			@Override
			String getInternalDefinition(Column c) {
				return join(c.getQuotedName(), dbFieldType());
			}

			@Override
			String getDefaultDefinition(Column c) {
				IntegerColumn ic = (IntegerColumn) c;
				String defaultStr = "";
				if (ic.getDefaultValue() != null) {
					defaultStr = DEFAULT + ic.getDefaultValue();
				}
				return defaultStr;
			}
		}

		);

		TYPES_DICT.put(FloatingColumn.class, new OraColumnDefiner() {

			@Override
			String dbFieldType() {
				return "real";
			}

			@Override
			String getInternalDefinition(Column c) {
				return join(c.getQuotedName(), dbFieldType());
			}

			@Override
			String getDefaultDefinition(Column c) {
				FloatingColumn ic = (FloatingColumn) c;
				String defaultStr = "";
				if (ic.getDefaultValue() != null) {
					defaultStr = DEFAULT + ic.getDefaultValue();
				}
				return defaultStr;
			}

		}

		);
		TYPES_DICT.put(StringColumn.class, new OraColumnDefiner() {

			@Override
			String dbFieldType() {
				return "varchar2";
			}

			// Пустая DEFAULT-строка не сочетается с NOT NULL в Oracle.
			@Override
			String nullable(Column c) {
				StringColumn ic = (StringColumn) c;
				return ("".equals(ic.getDefaultValue())) ? "null" : super
						.nullable(c);
			}

			@Override
			String getInternalDefinition(Column c) {
				StringColumn ic = (StringColumn) c;
				// See
				// http://stackoverflow.com/questions/414817/what-is-the-equivalent-of-varcharmax-in-oracle
				String fieldType = String.format(
						"%s(%s)",
						dbFieldType(),
						ic.isMax() ? Integer.toString(LENGTHFORMAX) : ic
								.getLength());
				return join(c.getQuotedName(), fieldType);
			}

			@Override
			String getDefaultDefinition(Column c) {
				StringColumn ic = (StringColumn) c;
				String defaultStr = "";
				if (ic.getDefaultValue() != null) {
					defaultStr = DEFAULT
							+ StringColumn.quoteString(ic.getDefaultValue());
				}
				return defaultStr;
			}

		});
		TYPES_DICT.put(BinaryColumn.class, new OraColumnDefiner() {
			@Override
			String dbFieldType() {
				return "blob";
			}

			@Override
			String getInternalDefinition(Column c) {
				return join(c.getQuotedName(), dbFieldType());
			}

			@Override
			String getDefaultDefinition(Column c) {
				BinaryColumn ic = (BinaryColumn) c;
				String defaultStr = "";
				if (ic.getDefaultValue() != null) {
					// Отрезаем 0x и закавычиваем
					defaultStr = String.format(DEFAULT + "'%s'", ic
							.getDefaultValue().substring(2));
				}
				return defaultStr;
			}
		});

		TYPES_DICT.put(DateTimeColumn.class, new OraColumnDefiner() {

			@Override
			String dbFieldType() {
				return "timestamp";
			}

			@Override
			String getInternalDefinition(Column c) {
				return join(c.getQuotedName(), dbFieldType());
			}

			@Override
			String getDefaultDefinition(Column c) {
				DateTimeColumn ic = (DateTimeColumn) c;
				String defaultStr = "";
				if (ic.isGetdate()) {
					defaultStr = DEFAULT + "sysdate";
				} else if (ic.getDefaultValue() != null) {
					DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
					defaultStr = String.format(DEFAULT + "date '%s'",
							df.format(ic.getDefaultValue()));
				}
				return defaultStr;
			}
		});
		TYPES_DICT.put(BooleanColumn.class, new OraColumnDefiner() {

			@Override
			String dbFieldType() {
				return "int";
			}

			@Override
			String getInternalDefinition(Column c) {
				return join(c.getQuotedName(), dbFieldType());
			}

			@Override
			String getDefaultDefinition(Column c) {
				BooleanColumn ic = (BooleanColumn) c;
				String defaultStr = "";
				if (ic.getDefaultValue() != null) {
					defaultStr = DEFAULT + (ic.getDefaultValue() ? "1" : "0");
				}
				return defaultStr;
			}

			@Override
			String getFullDefinition(Column c) {
				String check = String.format(
						"constraint \"chk_%s_%s_%s\" check (%s in (0, 1))", c
								.getParentTable().getGrain().getName(), c
								.getParentTable().getName(), c.getName(), c
								.getQuotedName());
				return join(getInternalDefinition(c), getDefaultDefinition(c),
						nullable(c), check);
			}

		});
	}

	/**
	 * Выполняемое действия для Identity атрибута при create/drop таблицы.
	 */
	private interface PostCreateDropTableCommand {
		void command(IntegerColumn column) throws CelestaException;
	}

	@Override
	boolean tableExists(Connection conn, String schema, String name)
			throws SQLException {
		if (schema == null || schema.isEmpty() || name == null
				|| name.isEmpty()) {
			return false;
		}

		PreparedStatement checkForTable = conn
				.prepareStatement(String
						.format("select count(*) from all_tables where owner = "
								+ "sys_context('userenv','session_user') and table_name = '%s_%s'",
								schema, name));
		ResultSet rs = checkForTable.executeQuery();
		try {
			if (rs.next()) {
				return rs.getInt(1) > 0;
			}
			return false;
		} finally {
			checkForTable.close();
		}
	}

	@Override
	boolean userTablesExist(Connection conn) throws SQLException {
		PreparedStatement pstmt = conn
				.prepareStatement("SELECT COUNT(*) FROM USER_TABLES");
		ResultSet rs = pstmt.executeQuery();
		try {
			rs.next();
			return rs.getInt(1) > 0;
		} finally {
			rs.close();
			pstmt.close();
		}
	}

	@Override
	void createSchemaIfNotExists(Connection conn, String schema)
			throws SQLException {
		// Ничего не делает для Oracle. Схемы имитируются префиксами на именах
		// таблиц.
	}

	@Override
	OraColumnDefiner getColumnDefiner(Column c) {
		return TYPES_DICT.get(c.getClass());
	}

	@Override
	PreparedStatement getOneFieldStatement(Connection conn, Column c)
			throws CelestaException {
		Table t = c.getParentTable();
		String sql = String.format("select %s from " + tableTemplate()
				+ " where %s and rownum = 1", c.getQuotedName(), t.getGrain()
				.getName(), t.getName(), getRecordWhereClause(t));
		return prepareStatement(conn, sql);
	}

	@Override
	PreparedStatement getOneRecordStatement(Connection conn, Table t)
			throws CelestaException {
		String sql = String.format("select %s from " + tableTemplate()
				+ " where %s and rownum = 1", getTableFieldsListExceptBLOBs(t),
				t.getGrain().getName(), t.getName(), getRecordWhereClause(t));
		return prepareStatement(conn, sql);
	}

	@Override
	PreparedStatement getRecordSetStatement(Connection conn, Table t,
			Map<String, AbstractFilter> filters, List<String> orderBy)
			throws CelestaException {
		// Соединяем полученные компоненты в стандартный запрос
		// SELECT..FROM..WHERE..ORDER BY
		String sql = getSelectFromOrderBy(t, getWhereClause(filters), orderBy);

		try {
			PreparedStatement result = conn.prepareStatement(sql);
			fillSetQueryParameters(filters, result);
			return result;
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
	}

	@Override
	PreparedStatement getInsertRecordStatement(Connection conn, Table t,
			boolean[] nullsMask) throws CelestaException {

		Iterator<String> columns = t.getColumns().keySet().iterator();
		// Создаём параметризуемую часть запроса, пропуская нулевые значения.
		StringBuilder fields = new StringBuilder();
		StringBuilder params = new StringBuilder();
		for (int i = 0; i < t.getColumns().size(); i++) {
			String c = columns.next();
			if (nullsMask[i])
				continue;
			if (params.length() > 0) {
				fields.append(", ");
				params.append(", ");
			}
			params.append("?");
			fields.append('"');
			fields.append(c);
			fields.append('"');
		}

		String sql = String.format("insert into " + tableTemplate()
				+ " (%s) values (%s)", t.getGrain().getName(), t.getName(),
				fields.toString(), params.toString());
		return prepareStatement(conn, sql);
	}

	@Override
	PreparedStatement getUpdateRecordStatement(Connection conn, Table t,
			boolean[] equalsMask) throws CelestaException {
		StringBuilder setClause = new StringBuilder();
		int i = 0;
		for (String c : t.getColumns().keySet()) {
			// Пропускаем ключевые поля и поля, не изменившие своего значения
			if (!(equalsMask[i] || t.getPrimaryKey().containsKey(c))) {
				if (setClause.length() > 0)
					setClause.append(", ");
				setClause.append(String.format("\"%s\" = ?", c));
			}
			i++;
		}

		String sql = String.format("update " + tableTemplate()
				+ " set %s where %s", t.getGrain().getName(), t.getName(),
				setClause.toString(), getRecordWhereClause(t));
		return prepareStatement(conn, sql);
	}

	@Override
	PreparedStatement getDeleteRecordStatement(Connection conn, Table t)
			throws CelestaException {
		String sql = String.format("delete " + tableTemplate() + " where %s", t
				.getGrain().getName(), t.getName(), getRecordWhereClause(t));
		return prepareStatement(conn, sql);
	}

	@Override
	public Map<DBIndexInfo, TreeMap<Short, String>> getIndices(Connection conn,
			Grain g) throws CelestaException {
		Map<DBIndexInfo, TreeMap<Short, String>> result = new HashMap<>();
		try {
			for (Table t : g.getTables().values()) {
				String tableName = String.format(tableTemplate(), g.getName(),
						t.getName());
				DatabaseMetaData metaData = conn.getMetaData();
				ResultSet rs = metaData.getIndexInfo(null, null, tableName,
						false, false);
				try {
					while (rs.next()) {
						String indName = rs.getString("INDEX_NAME");
						// Условие NON_UNIQUE нужно, чтобы не попадали в него
						// первичные ключи
						if (indName != null && rs.getBoolean("NON_UNIQUE")) {
							// Мы отрезаем "имягранулы_" от имени индекса.
							String grainPrefix = g.getName() + "_";
							if (indName.startsWith(grainPrefix))
								indName = indName.substring(grainPrefix
										.length());
							DBIndexInfo info = new DBIndexInfo(t.getName(),
									indName);
							TreeMap<Short, String> columns = result.get(info);
							if (columns == null) {
								columns = new TreeMap<>();
								result.put(info, columns);
							}
							columns.put(rs.getShort("ORDINAL_POSITION"),
									rs.getString("COLUMN_NAME"));
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

	@Override
	public Set<String> getColumns(Connection conn, Table t)
			throws CelestaException {
		Set<String> result = new LinkedHashSet<>();
		try {
			String tableName = String.format("%s_%s", t.getGrain().getName(),
					t.getName());
			DatabaseMetaData metaData = conn.getMetaData();
			ResultSet rs = metaData.getColumns(null, null, tableName, null);
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

	@Override
	PreparedStatement deleteRecordSetStatement(Connection conn, Table t,
			Map<String, AbstractFilter> filters) throws CelestaException {
		String whereClause = getWhereClause(filters);
		String sql = String.format("delete from " + tableTemplate() + " %s", t
				.getGrain().getName(), t.getName(),
				!whereClause.isEmpty() ? "where " + whereClause : "");
		try {
			PreparedStatement result = conn.prepareStatement(sql);
			if (filters != null) {
				fillSetQueryParameters(filters, result);
			}
			return result;
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
	}

	@Override
	public boolean isValidConnection(Connection conn, int timeout)
			throws CelestaException {
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("SELECT 1 FROM Dual");
			if (rs.next()) {
				return true;
			}
			return false;
		} catch (SQLException e) {
			return false;
		} finally {
			try {
				if (stmt != null)
					stmt.close();
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
				throw new CelestaException(e.getMessage());
			}
		}
	}

	private void createAutoincrement(Connection conn, Table table,
			IntegerColumn col) throws SQLException {
		String sequenceName = getSequenceName(table, col);
		String sql = "CREATE SEQUENCE " + sequenceName
				+ " START WITH 1 INCREMENT BY 1 MINVALUE 1 NOCACHE NOCYCLE";
		Statement stmt = conn.createStatement();
		try {
			stmt.execute(sql);
		} finally {
			stmt.close();
		}
		// Создание Trigger
		sql = String.format("CREATE OR REPLACE TRIGGER " + sequenceName
				+ " BEFORE INSERT ON " + tableTemplate()
				+ " FOR EACH ROW WHEN (new.%s is null) BEGIN SELECT "
				+ sequenceName + ".NEXTVAL INTO :new.%s FROM dual; END;", table
				.getGrain().getName(), table.getName(), col.getQuotedName(),
				col.getQuotedName());
		stmt = conn.createStatement();
		try {
			stmt.execute(sql);
		} finally {
			stmt.close();
		}
	}

	/*
	 * Trigger удаляется вместе с удалением таблицы
	 */
	private void deleteAutoincrement(Connection conn, Table table,
			IntegerColumn col) throws SQLException {
		// Удаление Sequence
		String sequenceName = getSequenceName(table, col);
		String sql = "DROP SEQUENCE " + sequenceName;
		Statement stmt = conn.createStatement();
		try {
			stmt.execute(sql);
		} finally {
			stmt.close();
		}
	}

	private String getSequenceName(Table table, Column col) {
		String sequenceName = String.format("\"%s_%s_inc\"", table.getGrain()
				.getName(), table.getName());
		return sequenceName;
	}

	private void postCreateDropTable(Connection conn, Table table,
			PostCreateDropTableCommand action) throws CelestaException {
		for (Column column : table.getColumns().values()) {
			if (column instanceof IntegerColumn) {
				IntegerColumn col = (IntegerColumn) column;
				if (col.isIdentity()) {
					action.command(col);
				}
			}
		}
	}

	@Override
	public void postCreateTable(final Connection conn, final Table table)
			throws CelestaException {
		postCreateDropTable(conn, table, new PostCreateDropTableCommand() {

			@Override
			public void command(IntegerColumn column) throws CelestaException {
				try {
					createAutoincrement(conn, table, column);
				} catch (SQLException e) {
					throw new CelestaException(e.getMessage());
				}
			}

		});
	}

	public void postDropTable(final Connection conn, final Table table)
			throws CelestaException {
		postCreateDropTable(conn, table, new PostCreateDropTableCommand() {

			@Override
			public void command(IntegerColumn column) throws CelestaException {
				try {
					deleteAutoincrement(conn, table, column);
				} catch (SQLException e) {
					throw new CelestaException(e.getMessage());
				}
			}

		});
	}

	@Override
	public String tableTemplate() {
		return "\"%s_%s\"";
	}

	@Override
	int getCurrentIdent(Connection conn, Table t) throws CelestaException {
		String sequenceName = "";
		for (Column col : t.getColumns().values())
			if (col instanceof IntegerColumn
					&& ((IntegerColumn) col).isIdentity()) {
				sequenceName = getSequenceName(t, col);
				break;
			}
		if ("".equals(sequenceName))
			throw new IllegalArgumentException("Table has no identity field");
		PreparedStatement stmt = prepareStatement(conn,
				String.format("SELECT %s.CURRVAL FROM DUAL", sequenceName));
		try {
			ResultSet rs = stmt.executeQuery();
			rs.next();
			return rs.getInt(1);
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
	}

	@Override
	String getCreateIndexSQL(Index index) {
		String grainName = index.getTable().getGrain().getName();
		String fieldList = getFieldList(index.getColumns().keySet());
		String sql = String.format("CREATE INDEX " + tableTemplate() + " ON "
				+ tableTemplate() + " (%s)", grainName, index.getName(),
				grainName, index.getTable().getName(), fieldList);
		return sql;
	}

	@Override
	String getDropIndexSQL(Grain g, DBIndexInfo dBIndexInfo) {
		String sql = String.format("DROP INDEX " + tableTemplate(),
				g.getName(), dBIndexInfo.getIndexName());
		return sql;
	}

	private boolean checkForBoolean(Connection conn, Column c)
			throws SQLException {
		PreparedStatement checkForBool = conn.prepareStatement(String.format(
				"SELECT SEARCH_CONDITION FROM ALL_CONSTRAINTS WHERE "
						+ "OWNER = sys_context('userenv','session_user')"
						+ " AND TABLE_NAME = '%s_%s'"
						+ "AND CONSTRAINT_TYPE = 'C'", c.getParentTable()
						.getGrain().getName(), c.getParentTable().getName()));
		try {
			ResultSet rs = checkForBool.executeQuery();
			while (rs.next()) {
				Matcher m = BOOLEAN_CHECK.matcher(rs.getString(1));
				if (m.find() && m.group(1).equals(c.getName()))
					return true;
			}
		} finally {
			checkForBool.close();
		}
		return false;

	}

	private boolean checkForIncrementTrigger(Connection conn, Column c)
			throws SQLException {
		PreparedStatement checkForTrigger = conn
				.prepareStatement(String
						.format("select TRIGGER_BODY  from all_triggers where owner = sys_context('userenv','session_user') "
								+ "and table_name = '%s_%s' and trigger_name = '%s_%s_inc' and triggering_event = 'INSERT'",
								c.getParentTable().getGrain().getName(), c
										.getParentTable().getName(), c
										.getParentTable().getGrain().getName(),
								c.getParentTable().getName()));
		try {
			ResultSet rs = checkForTrigger.executeQuery();
			if (rs.next()) {
				String body = rs.getString(1);
				if (body != null && body.contains(".NEXTVAL")
						&& body.contains("\"" + c.getName() + "\""))
					return true;
			}
		} finally {
			checkForTrigger.close();
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public DBColumnInfo getColumnInfo(Connection conn, Column c)
			throws CelestaException {
		try {
			String tableName = String.format("%s_%s", c.getParentTable()
					.getGrain().getName(), c.getParentTable().getName());
			DatabaseMetaData metaData = conn.getMetaData();
			ResultSet rs = metaData.getColumns(null, null, tableName,
					c.getName());
			DBColumnInfo result;
			try {
				if (rs.next()) {
					result = new DBColumnInfo();
					result.setName(rs.getString(COLUMN_NAME));
					String typeName = rs.getString("TYPE_NAME");

					if (typeName.startsWith("TIMESTAMP")) {
						result.setType(DateTimeColumn.class);
					} else if ("float".equalsIgnoreCase(typeName))
						result.setType(FloatingColumn.class);
					else {
						for (Class<?> cc : COLUMN_CLASSES)
							if (TYPES_DICT.get(cc).dbFieldType()
									.equalsIgnoreCase(typeName)) {
								result.setType((Class<? extends Column>) cc);
								break;
							}
					}
					if (IntegerColumn.class == result.getType()) {
						// В Oracle булевские столбцы имеют тот же тип данных,
						// что и INT-столбцы: просматриваем, есть ли на них
						// ограничение CHECK.
						if (checkForBoolean(conn, c))
							result.setType(BooleanColumn.class);
						// В Oracle признак IDENTITY имитируется триггером.
						// Просматриваем, есть ли на поле триггер, обладающий
						// признаками того, что это -- созданный Celesta
						// системный триггер.
						else if (checkForIncrementTrigger(conn, c))
							result.setIdentity(true);
					}
					result.setNullable(rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls);
					if (result.getType() == StringColumn.class) {
						result.setLength(rs.getInt("COLUMN_SIZE"));
						result.setMax(result.getLength() >= LENGTHFORMAX);
					}

				} else {
					return null;
				}
			} finally {
				rs.close();
			}
			// В Oracle JDBC не работает штатное поле для значения DEFAULT
			// ("COLUMN_DEF"),
			// поэтому извлекаем его самостоятельно.
			processDefaults(conn, c, result);

			return result;
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}

	}

	private void processDefaults(Connection conn, Column c, DBColumnInfo result)
			throws SQLException {
		ResultSet rs;
		PreparedStatement getDefault = conn.prepareStatement(String.format(
				"select DATA_DEFAULT from DBA_TAB_COLUMNS where "
						+ "owner = sys_context('userenv','session_user') "
						+ "and TABLE_NAME = '%s_%s' and COLUMN_NAME = '%s'", c
						.getParentTable().getGrain().getName(), c
						.getParentTable().getName(), c.getName()));
		try {
			rs = getDefault.executeQuery();
			if (!rs.next())
				return;
			String body = rs.getString(1);
			if (body == null || "null".equalsIgnoreCase(body))
				return;

			if (BooleanColumn.class == result.getType())
				body = "0".equals(body) ? "'FALSE'" : "'TRUE'";
			else if (DateTimeColumn.class == result.getType()) {
				if (body.toLowerCase().contains("sysdate"))
					body = "GETDATE()";
				else {
					Matcher m = DATE_PATTERN.matcher(body);
					if (m.find())
						body = String.format("'%s%s%s'", m.group(1),
								m.group(2), m.group(3));
				}
			} else if (BinaryColumn.class == result.getType()) {
				Matcher m = HEX_STRING.matcher(body);
				if (m.find())
					body = "0x" + m.group(1);
			} else {
				body = body.trim();
			}
			result.setDefaultValue(body);

		} finally {
			getDefault.close();
		}
	}

	@Override
	void updateColumn(Connection conn, Column c, DBColumnInfo actual)
			throws CelestaException {
		OraColumnDefiner definer = getColumnDefiner(c);

		// В Oracle нельзя снять default, можно только установить его в Null
		String defdef = definer.getDefaultDefinition(c);
		if ("".equals(defdef) && !"".equals(actual.getDefaultValue()))
			defdef = "default null";
		String def = OraColumnDefiner.join(definer.getInternalDefinition(c),
				defdef);

		// Явно задавать nullable в Oracle можно только если действительно надо
		// изменить
		if (actual.isNullable() != c.isNullable())
			def = OraColumnDefiner.join(def, definer.nullable(c));

		String sql = String.format("alter table " + tableTemplate()
				+ " modify (%s)", c.getParentTable().getGrain().getName(), c
				.getParentTable().getName(), def);

		System.out.println(sql);
		PreparedStatement stmt = prepareStatement(conn, sql);
		try {
			stmt.executeUpdate();
		} catch (SQLException e) {
			throw new CelestaException(
					"Cannot modify column %s on table %s.%s: %s", c.getName(),
					c.getParentTable().getGrain().getName(), c.getParentTable()
							.getName(), e.getMessage());

		}

	}
}

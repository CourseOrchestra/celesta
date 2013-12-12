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
import java.util.LinkedList;
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
import ru.curs.celesta.score.FKRule;
import ru.curs.celesta.score.FloatingColumn;
import ru.curs.celesta.score.ForeignKey;
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
	private static final Pattern TABLE_PATTERN = Pattern
			.compile("([a-zA-Z][a-zA-Z0-9]*)_([a-zA-Z_][a-zA-Z0-9_]*)");

	private static final Map<Class<? extends Column>, OraColumnDefiner> TYPES_DICT = new HashMap<>();

	private static final int MAX_CONSTRAINT_NAME = 30;
	private static final int HASH_MASK = 0xFFFF;

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
				return "number";
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
						"constraint %s check (%s in (0, 1))",
						getBooleanCheckName(c), c.getQuotedName());
				return join(getInternalDefinition(c), getDefaultDefinition(c),
						nullable(c), check);
			}

		});
	}

	@Override
	boolean tableExists(Connection conn, String schema, String name)
			throws CelestaException {
		if (schema == null || schema.isEmpty() || name == null
				|| name.isEmpty()) {
			return false;
		}
		String sql = String
				.format("select count(*) from all_tables where owner = "
						+ "sys_context('userenv','session_user') and table_name = '%s_%s'",
						schema, name);

		try {
			Statement checkForTable = conn.createStatement();
			ResultSet rs = checkForTable.executeQuery(sql);
			try {
				if (rs.next()) {
					return rs.getInt(1) > 0;
				}
				return false;
			} finally {
				checkForTable.close();
			}
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
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
			for (Table t : g.getTables().values())
				// В Oracle имеет смысл спрашивать про индексы только лишь
				// существующих таблиц.
				// В противном случае возникнет ошибка.
				if (tableExists(conn, g.getName(), t.getName())) {
					String tableName = String.format(tableTemplate(),
							g.getName(), t.getName());

					DatabaseMetaData metaData = conn.getMetaData();
					ResultSet rs = metaData.getIndexInfo(null, null, tableName,
							false, false);
					try {
						while (rs.next()) {
							String indName = rs.getString("INDEX_NAME");
							// Условие NON_UNIQUE нужно, чтобы не попадали в
							// него
							// первичные ключи
							if (indName != null && rs.getBoolean("NON_UNIQUE")) {
								// Мы отрезаем "имягранулы_" от имени индекса.
								String grainPrefix = g.getName() + "_";
								if (indName.startsWith(grainPrefix))
									indName = indName.substring(grainPrefix
											.length());
								DBIndexInfo info = new DBIndexInfo(t.getName(),
										indName);
								TreeMap<Short, String> columns = result
										.get(info);
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

	@Override
	PreparedStatement deleteRecordSetStatement(Connection conn, Table t,
			Map<String, AbstractFilter> filters) throws CelestaException {
		String whereClause = getWhereClause(t, filters);
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

	private String getSequenceName(Table table) {
		String sequenceName = String.format("\"%s_%s_inc\"", table.getGrain()
				.getName(), table.getName());
		return sequenceName;
	}

	@Override
	public String tableTemplate() {
		return "\"%s_%s\"";
	}

	@Override
	int getCurrentIdent(Connection conn, Table t) throws CelestaException {
		String sequenceName = getSequenceName(t);
		String sql = String.format("SELECT %s.CURRVAL FROM DUAL", sequenceName);
		try {
			Statement stmt = conn.createStatement();
			try {
				ResultSet rs = stmt.executeQuery(sql);
				rs.next();
				return rs.getInt(1);
			} finally {
				stmt.close();
			}
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
				body = "0".equals(body.trim()) ? "'FALSE'" : "'TRUE'";
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

		if (actual.getType() == BooleanColumn.class
				&& !(c instanceof BooleanColumn)) {
			// Тип Boolean меняется на что-то другое, надо сброить constraint
			String check = String.format(ALTER_TABLE + tableTemplate()
					+ " drop constraint %s", c.getParentTable().getGrain()
					.getName(), c.getParentTable().getName(),
					getBooleanCheckName(c));
			runUpdateColumnSQL(conn, c, check);
		}

		OraColumnDefiner definer = getColumnDefiner(c);

		// В Oracle нельзя снять default, можно только установить его в Null
		String defdef = definer.getDefaultDefinition(c);
		if ("".equals(defdef) && !"".equals(actual.getDefaultValue()))
			defdef = "default null";

		// В Oracle, если меняешь blob-поле, то в alter table не надо
		// указывать его тип (будет ошибка).
		String def;
		if (actual.getType() == BinaryColumn.class && c instanceof BinaryColumn) {
			def = OraColumnDefiner.join(c.getQuotedName(), defdef);
		} else {
			def = OraColumnDefiner.join(definer.getInternalDefinition(c),
					defdef);
		}

		// Явно задавать nullable в Oracle можно только если действительно надо
		// изменить
		if (actual.isNullable() != c.isNullable())
			def = OraColumnDefiner.join(def, definer.nullable(c));

		String sql = String.format(ALTER_TABLE + tableTemplate()
				+ " modify (%s)", c.getParentTable().getGrain().getName(), c
				.getParentTable().getName(), def);
		runUpdateColumnSQL(conn, c, sql);

		if (c instanceof BooleanColumn
				&& actual.getType() != BooleanColumn.class) {
			// Тип поменялся на Boolean, надо добавить constraint
			String check = String.format(ALTER_TABLE + tableTemplate()
					+ " add constraint %s check (%s in (0, 1))", c
					.getParentTable().getGrain().getName(), c.getParentTable()
					.getName(), getBooleanCheckName(c), c.getQuotedName());
			runUpdateColumnSQL(conn, c, check);

		} else if (c instanceof IntegerColumn)
			try {
				manageAutoIncrement(conn, c.getParentTable());
			} catch (SQLException e) {
				throw new CelestaException(
						"Failed to update field %s.%s.%s: %s", c
								.getParentTable().getGrain().getName(), c
								.getParentTable().getName(), c.getName(),
						e.getMessage());
			}
	}

	private static String getBooleanCheckName(Column c) {
		String result = String.format("\"chk_%s_%s_%s\"", c.getParentTable()
				.getGrain().getName(), c.getParentTable().getName(),
				c.getName());
		if (result.length() > MAX_CONSTRAINT_NAME) {
			result = String.format("%s%04X",
					result.substring(0, MAX_CONSTRAINT_NAME - 4),
					result.hashCode() & HASH_MASK);
		}
		return result;
	}

	@Override
	void manageAutoIncrement(Connection conn, Table t) throws SQLException {
		// 1. Firstly, we have to clean up table from any auto-increment
		// triggers
		String sequenceName = getSequenceName(t);
		String sql = String.format("drop trigger %s", sequenceName);
		PreparedStatement stmt = conn.prepareStatement(sql);
		try {
			stmt.executeUpdate();
		} catch (SQLException e) {
			// do nothing
			sql = "";
		} finally {
			stmt.close();
		}

		// 2. Check if table has IDENTITY field, if it doesn't, no need to
		// proceed.
		IntegerColumn ic = null;
		for (Column c : t.getColumns().values())
			if (c instanceof IntegerColumn && ((IntegerColumn) c).isIdentity()) {
				ic = (IntegerColumn) c;
				break;
			}
		if (ic == null)
			return;

		// 2. Now, we know that we surely have IDENTITY field, and we have to
		// be sure that we have an appropriate sequence.
		boolean hasSequence = false;
		stmt = conn
				.prepareStatement(String
						.format("select count(*) from all_sequences where sequence_owner = "
								+ "sys_context('userenv','session_user') and sequence_name = '%s_%s_inc'",
								t.getGrain().getName(), t.getName()));
		ResultSet rs = stmt.executeQuery();
		try {
			hasSequence = rs.next() && rs.getInt(1) > 0;
		} finally {
			stmt.close();
		}
		if (!hasSequence) {
			sql = String
					.format("CREATE SEQUENCE %s"
							+ " START WITH 1 INCREMENT BY 1 MINVALUE 1 NOCACHE NOCYCLE",
							sequenceName);
			stmt = conn.prepareStatement(sql);
			try {
				stmt.executeUpdate();
			} finally {
				stmt.close();
			}
		}

		// 3. Now we have to create or replace the auto-increment trigger
		sql = String.format("CREATE OR REPLACE TRIGGER " + sequenceName
				+ " BEFORE INSERT ON " + tableTemplate()
				+ " FOR EACH ROW WHEN (new.%s is null) BEGIN SELECT "
				+ sequenceName + ".NEXTVAL INTO :new.%s FROM dual; END;", t
				.getGrain().getName(), t.getName(), ic.getQuotedName(), ic
				.getQuotedName());
		Statement s = conn.createStatement();
		try {
			s.execute(sql);
		} finally {
			stmt.close();
		}
	}

	@Override
	void dropAutoIncrement(Connection conn, Table t) throws SQLException {
		// Удаление Sequence
		String sequenceName = getSequenceName(t);
		String sql = "DROP SEQUENCE " + sequenceName;
		Statement stmt = conn.createStatement();
		try {
			stmt.execute(sql);
		} catch (SQLException e) {
			// do nothing
			sql = "";
		} finally {

			stmt.close();
		}

	}

	@Override
	DBPKInfo getPKInfo(Connection conn, Table t) throws CelestaException {
		DBPKInfo result = new DBPKInfo();
		try {
			String sql = String
					.format("select cons.constraint_name, column_name from all_constraints cons "
							+ "inner join all_cons_columns cols on cons.constraint_name = cols.constraint_name  "
							+ "and cons.owner = cols.owner where "
							+ "cons.owner = sys_context('userenv','session_user') "
							+ "and cons.table_name = '%s_%s'"
							+ " and cons.constraint_type = 'P' order by cols.position",
							t.getGrain().getName(), t.getName());
			Statement check = conn.createStatement();
			ResultSet rs = check.executeQuery(sql);
			try {
				while (rs.next()) {
					result.setName(rs.getString(1));
					result.getColumnNames().add(rs.getString(2));
				}
			} finally {
				rs.close();
				check.close();
			}
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}

		return result;

	}

	@Override
	void dropPK(Connection conn, Table t, String pkName)
			throws CelestaException {
		String sql = String.format(
				"alter table \"%s_%s\" drop constraint \"%s\"", t.getGrain()
						.getName(), t.getName(), pkName);
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

	@Override
	void createPK(Connection conn, Table t) throws CelestaException {
		StringBuilder sql = new StringBuilder();
		sql.append(String.format("alter table \"%s_%s\" add constraint \"%s\" "
				+ " primary key (", t.getGrain().getName(), t.getName(),
				t.getPkConstraintName()));
		boolean multiple = false;
		for (String s : t.getPrimaryKey().keySet()) {
			if (multiple)
				sql.append(", ");
			sql.append('"');
			sql.append(s);
			sql.append('"');
			multiple = true;
		}
		sql.append(")");

		try {
			Statement stmt = conn.createStatement();
			try {
				stmt.executeUpdate(sql.toString());
			} finally {
				stmt.close();
			}
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}

	}

	@Override
	List<DBFKInfo> getFKInfo(Connection conn, Grain g) throws CelestaException {
		String sql = String
				.format("select cols.constraint_name, cols.table_name table_name, "
						+ "ref.table_name ref_table_name, cons.delete_rule, cols.column_name "
						+ "from all_constraints cons inner join all_cons_columns cols "
						+ "on cols.owner = cons.owner and cols.constraint_name = cons.constraint_name "
						+ "  and cols.table_name = cons.table_name "
						+ "inner join all_constraints ref on ref.owner = cons.owner "
						+ "  and ref.constraint_name = cons.r_constraint_name "
						+ "where cons.constraint_type = 'R' "
						+ "and cons.owner = sys_context('userenv','session_user') "
						+ "and ref.constraint_type = 'P' "
						+ "and  cons.table_name like '%s_%%' order by cols.constraint_name, cols.position",
						g.getName());

		// System.out.println(sql);
		List<DBFKInfo> result = new LinkedList<>();
		try {
			Statement stmt = conn.createStatement();
			try {
				DBFKInfo i = null;
				ResultSet rs = stmt.executeQuery(sql);
				while (rs.next()) {
					String fkName = rs.getString("CONSTRAINT_NAME");
					if (i == null || !i.getName().equals(fkName)) {
						i = new DBFKInfo(fkName);
						result.add(i);
						String tableName = rs.getString("TABLE_NAME");
						Matcher m = TABLE_PATTERN.matcher(tableName);
						m.find();
						i.setTableName(m.group(2));
						tableName = rs.getString("REF_TABLE_NAME");
						m = TABLE_PATTERN.matcher(tableName);
						m.find();
						i.setRefGrainName(m.group(1));
						i.setRefTableName(m.group(2));
						i.setUpdateRule(getUpdateBehaviour(conn, tableName,
								fkName));
						i.setDeleteRule(getFKRule(rs.getString("DELETE_RULE")));
					}
					i.getColumnNames().add(rs.getString(COLUMN_NAME));
				}
			} finally {
				stmt.close();
			}
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
		return result;

	}

	private FKRule getUpdateBehaviour(Connection conn, String tableName,
			String fkName) throws SQLException {
		// now we are looking for triggers that define update
		// rule
		String sql = String
				.format("select trigger_name from all_triggers "
						+ "where owner = sys_context('userenv','session_user') "
						+ "and table_name = '%s' and trigger_name like '%%_%s' and triggering_event = 'UPDATE'",
						tableName, fkName);
		Statement stmt = conn.createStatement();
		try {
			ResultSet rs = stmt.executeQuery(sql);
			if (rs.next()) {
				sql = rs.getString("TRIGGER_NAME");
				if (sql.startsWith("csc_"))
					return FKRule.CASCADE;
				else if (sql.startsWith("snl_"))
					return FKRule.SET_NULL;
			}
			return FKRule.NO_ACTION;
		} finally {
			stmt.close();
		}
	}

	@Override
	void processCreateUpdateRule(ForeignKey fk, LinkedList<StringBuilder> sql) {
		StringBuilder sb;

		// Clean up unwanted triggers
		switch (fk.getUpdateRule()) {
		case CASCADE:
			sb = new StringBuilder("drop trigger \"snl_");
			sb.append(fk.getConstraintName());
			sb.append("\"");
			sql.add(sb);
			break;
		case SET_NULL:
			sb = new StringBuilder("drop trigger \"csc_");
			sb.append(fk.getConstraintName());
			sb.append("\"");
			sql.add(sb);
			break;
		case NO_ACTION:
		default:
			sb = new StringBuilder("drop trigger \"snl_");
			sb.append(fk.getConstraintName());
			sb.append("\"");
			sql.add(sb);
			sb = new StringBuilder("drop trigger \"csc_");
			sb.append(fk.getConstraintName());
			sb.append("\"");
			sql.add(sb);
			return;
		}

		sb = new StringBuilder();
		sb.append("create or replace trigger \"");
		if (fk.getUpdateRule() == FKRule.CASCADE) {
			sb.append("csc_");
		} else {
			sb.append("snl_");
		}

		sb.append(fk.getConstraintName());
		sb.append("\" after update of ");
		Table t = fk.getReferencedTable();
		boolean needComma = false;
		for (Column c : t.getPrimaryKey().values()) {
			if (needComma)
				sb.append(", ");
			sb.append(c.getQuotedName());
			needComma = true;
		}
		sb.append(String.format(" on \"%s_%s\"", t.getGrain().getName(),
				t.getName()));
		sb.append(String.format(" for each row begin\n  update \"%s_%s\" set ",
				fk.getParentTable().getGrain().getName(), fk.getParentTable()
						.getName()));

		Iterator<Column> i1 = fk.getColumns().values().iterator();
		Iterator<Column> i2 = t.getPrimaryKey().values().iterator();
		needComma = false;
		while (i1.hasNext()) {
			sb.append(needComma ? ",\n    " : "\n    ");
			needComma = true;
			sb.append(i1.next().getQuotedName());
			sb.append(" = :new.");
			sb.append(i2.next().getQuotedName());
		}
		sb.append("\n  where ");
		i1 = fk.getColumns().values().iterator();
		i2 = t.getPrimaryKey().values().iterator();
		needComma = false;
		while (i1.hasNext()) {
			sb.append(needComma ? ",\n    " : "\n    ");
			needComma = true;
			sb.append(i1.next().getQuotedName());
			if (fk.getUpdateRule() == FKRule.CASCADE) {
				sb.append(" = :old.");
				sb.append(i2.next().getQuotedName());
			} else {
				sb.append(" = null");
			}
		}
		sb.append(";\nend;");
		sql.add(sb);
	}

	@Override
	void processDropUpdateRule(LinkedList<String> sqlQueue, String fkName) {
		String sql = String.format("drop trigger \"snl_%s\"", fkName);
		sqlQueue.add(sql);
		sql = String.format("drop trigger \"csc_%s\"", fkName);
		sqlQueue.add(sql);
	}

	@Override
	String getLimitedSQL(Table t, String whereClause, String orderBy,
			long offset, long rowCount) {
		if (offset == 0 && rowCount == 0)
			throw new IllegalArgumentException();
		String sql;
		if (offset == 0) {
			// No offset -- simpler query
			sql = String.format(
					"with a as (%s) select a.* from a where rownum <= %d",
					getSelectFromOrderBy(t, whereClause, orderBy), rowCount);
		} else if (rowCount == 0) {
			// No rowCount -- simpler query
			sql = String.format(
					"with a as (%s) select * from (select a.*, ROWNUM rnum "
							+ "from a) where rnum >= %d order by rnum",
					getSelectFromOrderBy(t, whereClause, orderBy), offset + 1L);

		} else {
			sql = String
					.format("with a as (%s) select * from (select a.*, ROWNUM rnum "
							+ "from a where rownum <= %d) where rnum >= %d order by rnum",
							getSelectFromOrderBy(t, whereClause, orderBy),
							offset + rowCount, offset + 1L);
		}
		return sql;
	}
}

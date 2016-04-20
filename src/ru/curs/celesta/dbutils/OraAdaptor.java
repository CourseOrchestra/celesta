/*
   (с) 2013 ООО "КУРС-ИТ"  

   Этот файл — часть КУРС:Celesta.
   
   КУРС:Celesta — свободная программа: вы можете перераспространять ее и/или изменять
   ее на условиях Стандартной общественной лицензии GNU в том виде, в каком
   она была опубликована Фондом свободного программного обеспечения; либо
   версии 3 лицензии, либо (по вашему выбору) любой более поздней версии.

   Эта программа распространяется в надежде, что она будет полезной,
   но БЕЗО ВСЯКИХ ГАРАНТИЙ; даже без неявной гарантии ТОВАРНОГО ВИДА
   или ПРИГОДНОСТИ ДЛЯ ОПРЕДЕЛЕННЫХ ЦЕЛЕЙ. Подробнее см. в Стандартной
   общественной лицензии GNU.

   Вы должны были получить копию Стандартной общественной лицензии GNU
   вместе с этой программой. Если это не так, см. http://www.gnu.org/licenses/.

   
   Copyright 2013, COURSE-IT Ltd.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   You should have received a copy of the GNU General Public License
   along with this program.  If not, see http://www.gnu.org/licenses/.

 */

package ru.curs.celesta.dbutils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ru.curs.celesta.CelestaException;
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
import ru.curs.celesta.score.NamedElement;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.SQLGenerator;
import ru.curs.celesta.score.StringColumn;
import ru.curs.celesta.score.Table;
import ru.curs.celesta.score.TableRef;
import ru.curs.celesta.score.View;

/**
 * Адаптер Oracle Database.
 */
final class OraAdaptor extends DBAdaptor {
	private static final String SELECT_S_FROM = "select %s from ";

	private static final String SELECT_TRIGGER_BODY = "select TRIGGER_BODY  from all_triggers "
			+ "where owner = sys_context('userenv','session_user') ";

	private static final String CSC = "csc_";

	private static final String SNL = "snl_";

	private static final String DROP_TRIGGER = "drop trigger \"";

	private static final String TABLE_TEMPLATE = "\"%s_%s\"";

	private static final Pattern BOOLEAN_CHECK = Pattern.compile("\"([^\"]+)\" *[iI][nN] *\\( *0 *, *1 *\\)");
	private static final Pattern DATE_PATTERN = Pattern.compile("'(\\d\\d\\d\\d)-([01]\\d)-([0123]\\d)'");
	private static final Pattern HEX_STRING = Pattern.compile("'([0-9A-F]+)'");
	private static final Pattern TABLE_PATTERN = Pattern.compile("([a-zA-Z][a-zA-Z0-9]*)_([a-zA-Z_][a-zA-Z0-9_]*)");

	private static final Map<Class<? extends Column>, OraColumnDefiner> TYPES_DICT = new HashMap<>();

	/**
	 * Определитель колонок для Oracle, учитывающий тот факт, что в Oracle
	 * DEFAULT должен идти до NOT NULL.
	 */
	abstract static class OraColumnDefiner extends ColumnDefiner {
		abstract String getInternalDefinition(Column c);

		@Override
		String getFullDefinition(Column c) {
			return join(getInternalDefinition(c), getDefaultDefinition(c), nullable(c));
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
				return "nvarchar2";
			}

			// Пустая DEFAULT-строка не сочетается с NOT NULL в Oracle.
			@Override
			String nullable(Column c) {
				StringColumn ic = (StringColumn) c;
				return ("".equals(ic.getDefaultValue())) ? "null" : super.nullable(c);
			}

			@Override
			String getInternalDefinition(Column c) {
				StringColumn ic = (StringColumn) c;
				String fieldType = ic.isMax() ? "nclob" : String.format("%s(%s)", dbFieldType(), ic.getLength());
				return join(c.getQuotedName(), fieldType);
			}

			@Override
			String getDefaultDefinition(Column c) {
				StringColumn ic = (StringColumn) c;
				String defaultStr = "";
				if (ic.getDefaultValue() != null) {
					defaultStr = DEFAULT + StringColumn.quoteString(ic.getDefaultValue());
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
					defaultStr = String.format(DEFAULT + "'%s'", ic.getDefaultValue().substring(2));
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
					defaultStr = String.format(DEFAULT + "date '%s'", df.format(ic.getDefaultValue()));
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
				String check = String.format("constraint %s check (%s in (0, 1))", getBooleanCheckName(c),
						c.getQuotedName());
				return join(getInternalDefinition(c), getDefaultDefinition(c), nullable(c), check);
			}

		});
	}

	@Override
	boolean tableExists(Connection conn, String schema, String name) throws CelestaException {
		if (schema == null || schema.isEmpty() || name == null || name.isEmpty()) {
			return false;
		}
		String sql = String.format("select count(*) from all_tables where owner = "
				+ "sys_context('userenv','session_user') and table_name = '%s_%s'", schema, name);

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
		PreparedStatement pstmt = conn.prepareStatement("SELECT COUNT(*) FROM USER_TABLES");
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
	void createSchemaIfNotExists(Connection conn, String schema) throws SQLException {
		// Ничего не делает для Oracle. Схемы имитируются префиксами на именах
		// таблиц.
	}

	@Override
	OraColumnDefiner getColumnDefiner(Column c) {
		return TYPES_DICT.get(c.getClass());
	}

	@Override
	PreparedStatement getOneFieldStatement(Connection conn, Column c) throws CelestaException {
		Table t = c.getParentTable();
		String sql = String.format(SELECT_S_FROM + tableTemplate() + " where %s and rownum = 1", c.getQuotedName(),
				t.getGrain().getName(), t.getName(), getRecordWhereClause(t));
		return prepareStatement(conn, sql);
	}

	@Override
	PreparedStatement getOneRecordStatement(Connection conn, Table t) throws CelestaException {
		String sql = String.format(SELECT_S_FROM + tableTemplate() + " where %s and rownum = 1",
				getTableFieldsListExceptBLOBs(t), t.getGrain().getName(), t.getName(), getRecordWhereClause(t));
		return prepareStatement(conn, sql);
	}

	@Override
	PreparedStatement getInsertRecordStatement(Connection conn, Table t, boolean[] nullsMask) throws CelestaException {

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

		String sql = String.format("insert into " + tableTemplate() + " (%s) values (%s)", t.getGrain().getName(),
				t.getName(), fields.toString(), params.toString());
		return prepareStatement(conn, sql);
	}

	@Override
	PreparedStatement getDeleteRecordStatement(Connection conn, Table t) throws CelestaException {
		String sql = String.format("delete " + tableTemplate() + " where %s", t.getGrain().getName(), t.getName(),
				getRecordWhereClause(t));
		return prepareStatement(conn, sql);
	}

	@Override
	public Set<String> getColumns(Connection conn, Table t) throws CelestaException {
		Set<String> result = new LinkedHashSet<>();
		try {
			String tableName = String.format("%s_%s", t.getGrain().getName(), t.getName());
			String sql = String.format(
					"SELECT column_name FROM user_tab_cols WHERE table_name = '%s' order by column_id", tableName);
			// System.out.println(sql);
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			try {
				while (rs.next()) {
					String rColumnName = rs.getString(COLUMN_NAME);
					result.add(rColumnName);
				}
			} finally {
				rs.close();
			}
			stmt.close();
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
		return result;
	}

	@Override
	PreparedStatement deleteRecordSetStatement(Connection conn, Table t, Map<String, AbstractFilter> filters,
			Expr complexFilter) throws CelestaException {
		String whereClause = getWhereClause(t, filters, complexFilter);
		String sql = String.format("delete from " + tableTemplate() + " %s", t.getGrain().getName(), t.getName(),
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
	public boolean isValidConnection(Connection conn, int timeout) throws CelestaException {
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

	@Override
	public String tableTemplate() {
		return TABLE_TEMPLATE;
	}

	@Override
	int getCurrentIdent(Connection conn, Table t) throws CelestaException {
		String sequenceName = getSequenceName(t);
		String sql = String.format("SELECT \"%s\".CURRVAL FROM DUAL", sequenceName);
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
	String[] getCreateIndexSQL(Index index) {
		String grainName = index.getTable().getGrain().getName();
		String fieldList = getFieldList(index.getColumns().keySet());
		String sql = String.format("CREATE INDEX " + tableTemplate() + " ON " + tableTemplate() + " (%s)", grainName,
				index.getName(), grainName, index.getTable().getName(), fieldList);
		String[] result = {sql};
		return result;
	}

	@Override
	String[] getDropIndexSQL(Grain g, DBIndexInfo dBIndexInfo) {
		String sql;
		if (dBIndexInfo.getIndexName().startsWith("##")) {
			sql = String.format("DROP INDEX %s", dBIndexInfo.getIndexName().substring(2));
		} else {
			sql = String.format("DROP INDEX " + tableTemplate(), g.getName(), dBIndexInfo.getIndexName());
		}
		String[] result = {sql};
		return result;
	}

	private boolean checkForBoolean(Connection conn, Column c) throws SQLException {
		String sql = String.format(
				"SELECT SEARCH_CONDITION FROM ALL_CONSTRAINTS WHERE " + "OWNER = sys_context('userenv','session_user')"
						+ " AND TABLE_NAME = '%s_%s'" + "AND CONSTRAINT_TYPE = 'C'",
				c.getParentTable().getGrain().getName(), c.getParentTable().getName());
		// System.out.println(sql);
		PreparedStatement checkForBool = conn.prepareStatement(sql);
		try {
			ResultSet rs = checkForBool.executeQuery();
			while (rs.next()) {
				String buf = rs.getString(1);
				Matcher m = BOOLEAN_CHECK.matcher(buf);
				if (m.find() && m.group(1).equals(c.getName()))
					return true;
			}
		} finally {
			checkForBool.close();
		}
		return false;

	}

	private boolean checkForIncrementTrigger(Connection conn, Column c) throws SQLException {
		String sql = String.format(
				SELECT_TRIGGER_BODY
						+ "and table_name = '%s_%s' and trigger_name = '%s' and triggering_event = 'INSERT'",
				c.getParentTable().getGrain().getName(), c.getParentTable().getName(),
				getSequenceName(c.getParentTable()));
		// System.out.println(sql);
		PreparedStatement checkForTrigger = conn.prepareStatement(sql);
		try {
			ResultSet rs = checkForTrigger.executeQuery();
			if (rs.next()) {
				String body = rs.getString(1);
				if (body != null && body.contains(".NEXTVAL") && body.contains("\"" + c.getName() + "\""))
					return true;
			}
		} finally {
			checkForTrigger.close();
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public DBColumnInfo getColumnInfo(Connection conn, Column c) throws CelestaException {
		try {
			String tableName = String.format("%s_%s", c.getParentTable().getGrain().getName(),
					c.getParentTable().getName());
			String sql = String.format(
					"SELECT COLUMN_NAME, DATA_TYPE, NULLABLE, CHAR_LENGTH "
							+ "FROM user_tab_cols	WHERE table_name = '%s' and COLUMN_NAME = '%s'",
					tableName, c.getName());
			// System.out.println(sql);
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			DBColumnInfo result;
			try {
				if (rs.next()) {
					result = new DBColumnInfo();
					result.setName(rs.getString(COLUMN_NAME));
					String typeName = rs.getString("DATA_TYPE");

					if (typeName.startsWith("TIMESTAMP")) {
						result.setType(DateTimeColumn.class);
					} else if ("float".equalsIgnoreCase(typeName)) {
						result.setType(FloatingColumn.class);
					} else if ("nclob".equalsIgnoreCase(typeName)) {
						result.setType(StringColumn.class);
						result.setMax(true);
					} else {
						for (Class<?> cc : COLUMN_CLASSES)
							if (TYPES_DICT.get(cc).dbFieldType().equalsIgnoreCase(typeName)) {
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
					result.setNullable("Y".equalsIgnoreCase(rs.getString("NULLABLE")));
					if (result.getType() == StringColumn.class) {
						result.setLength(rs.getInt("CHAR_LENGTH"));
					}

				} else {
					return null;
				}
			} finally {
				rs.close();
				stmt.close();
			}
			// Извлекаем значение DEFAULT отдельно.
			processDefaults(conn, c, result);

			return result;
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}

	}

	private void processDefaults(Connection conn, Column c, DBColumnInfo result) throws SQLException {
		ResultSet rs;
		PreparedStatement getDefault = conn.prepareStatement(String.format(
				"select DATA_DEFAULT from DBA_TAB_COLUMNS where " + "owner = sys_context('userenv','session_user') "
						+ "and TABLE_NAME = '%s_%s' and COLUMN_NAME = '%s'",
				c.getParentTable().getGrain().getName(), c.getParentTable().getName(), c.getName()));
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
						body = String.format("'%s%s%s'", m.group(1), m.group(2), m.group(3));
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

	private boolean isNclob(Column c) {
		return c instanceof StringColumn && ((StringColumn) c).isMax();
	}

	@Override
	void updateColumn(Connection conn, Column c, DBColumnInfo actual) throws CelestaException {
		dropVersioningTrigger(conn, c.getParentTable());
		if (actual.getType() == BooleanColumn.class && !(c instanceof BooleanColumn)) {
			// Тип Boolean меняется на что-то другое, надо сбросить constraint
			String check = String.format(ALTER_TABLE + tableTemplate() + " drop constraint %s",
					c.getParentTable().getGrain().getName(), c.getParentTable().getName(), getBooleanCheckName(c));
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
			def = OraColumnDefiner.join(definer.getInternalDefinition(c), defdef);
		}

		// Явно задавать nullable в Oracle можно только если действительно надо
		// изменить
		if (actual.isNullable() != c.isNullable())
			def = OraColumnDefiner.join(def, definer.nullable(c));

		// Перенос из NCLOB и в NCLOB надо производить с осторожностью

		if (fromOrToNClob(c, actual)) {

			String tempName = "\"" + c.getName() + "2\"";
			String sql = String.format(ALTER_TABLE + tableTemplate() + " add %s",
					c.getParentTable().getGrain().getName(), c.getParentTable().getName(), columnDef(c));
			sql = sql.replace(c.getQuotedName(), tempName);
			// System.out.println(sql);
			runUpdateColumnSQL(conn, c, sql);
			sql = String.format("update " + tableTemplate() + " set %s = \"%s\"",
					c.getParentTable().getGrain().getName(), c.getParentTable().getName(), tempName, c.getName());
			// System.out.println(sql);
			runUpdateColumnSQL(conn, c, sql);
			sql = String.format(ALTER_TABLE + tableTemplate() + " drop column %s",
					c.getParentTable().getGrain().getName(), c.getParentTable().getName(), c.getQuotedName());
			// System.out.println(sql);
			runUpdateColumnSQL(conn, c, sql);
			sql = String.format(ALTER_TABLE + tableTemplate() + " rename column %s to %s",
					c.getParentTable().getGrain().getName(), c.getParentTable().getName(), tempName, c.getQuotedName());
			// System.out.println(sql);
			runUpdateColumnSQL(conn, c, sql);
		} else {

			String sql = String.format(ALTER_TABLE + tableTemplate() + " modify (%s)",
					c.getParentTable().getGrain().getName(), c.getParentTable().getName(), def);

			runUpdateColumnSQL(conn, c, sql);
		}
		if (c instanceof BooleanColumn && actual.getType() != BooleanColumn.class) {
			// Тип поменялся на Boolean, надо добавить constraint
			String check = String.format(ALTER_TABLE + tableTemplate() + " add constraint %s check (%s in (0, 1))",
					c.getParentTable().getGrain().getName(), c.getParentTable().getName(), getBooleanCheckName(c),
					c.getQuotedName());
			runUpdateColumnSQL(conn, c, check);

		}
	}

	public boolean fromOrToNClob(Column c, DBColumnInfo actual) {
		return (actual.isMax() || isNclob(c)) && !(actual.isMax() && isNclob(c));
	}

	private static String getFKTriggerName(String prefix, String fkName) {
		String result = prefix + fkName;
		result = NamedElement.limitName(result);
		return result;
	}

	private static String getBooleanCheckName(Column c) {
		String result = String.format("chk_%s_%s_%s", c.getParentTable().getGrain().getName(),
				c.getParentTable().getName(), c.getName());
		result = NamedElement.limitName(result);
		return "\"" + result + "\"";
	}

	private static String getSequenceName(Table table) {
		String result = String.format("%s_%s_inc", table.getGrain().getName(), table.getName());
		result = NamedElement.limitName(result);
		return result;
	}

	@Override
	void manageAutoIncrement(Connection conn, Table t) throws SQLException {
		// 1. Firstly, we have to clean up table from any auto-increment
		// triggers
		String sequenceName = getSequenceName(t);
		String sql = String.format(DROP_TRIGGER + "%s\"", sequenceName);
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
		IntegerColumn ic = findIdentityField(t);
		if (ic == null)
			return;

		// 2. Now, we know that we surely have IDENTITY field, and we have to
		// be sure that we have an appropriate sequence.
		boolean hasSequence = false;
		stmt = conn
				.prepareStatement(
						String.format(
								"select count(*) from all_sequences where sequence_owner = "
										+ "sys_context('userenv','session_user') and sequence_name = '%s'",
								sequenceName));
		ResultSet rs = stmt.executeQuery();
		try {
			hasSequence = rs.next() && rs.getInt(1) > 0;
		} finally {
			stmt.close();
		}
		if (!hasSequence) {
			sql = String.format("CREATE SEQUENCE \"%s\"" + " START WITH 1 INCREMENT BY 1 MINVALUE 1 NOCACHE NOCYCLE",
					sequenceName);
			stmt = conn.prepareStatement(sql);
			try {
				stmt.executeUpdate();
			} finally {
				stmt.close();
			}
		}

		// 3. Now we have to create or replace the auto-increment trigger
		sql = String.format(
				"CREATE OR REPLACE TRIGGER \"" + sequenceName + "\" BEFORE INSERT ON " + tableTemplate()
						+ " FOR EACH ROW WHEN (new.%s is null) BEGIN SELECT \"" + sequenceName
						+ "\".NEXTVAL INTO :new.%s FROM dual; END;",
				t.getGrain().getName(), t.getName(), ic.getQuotedName(), ic.getQuotedName());

		// System.out.println(sql);
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
		String sql = "DROP SEQUENCE \"" + sequenceName + "\"";
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
			String sql = String.format("select cons.constraint_name, column_name from all_constraints cons "
					+ "inner join all_cons_columns cols on cons.constraint_name = cols.constraint_name  "
					+ "and cons.owner = cols.owner where " + "cons.owner = sys_context('userenv','session_user') "
					+ "and cons.table_name = '%s_%s'" + " and cons.constraint_type = 'P' order by cols.position",
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
	void dropPK(Connection conn, Table t, String pkName) throws CelestaException {
		String sql = String.format("alter table \"%s_%s\" drop constraint \"%s\"", t.getGrain().getName(), t.getName(),
				pkName);
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
		sql.append(String.format("alter table \"%s_%s\" add constraint \"%s\" " + " primary key (",
				t.getGrain().getName(), t.getName(), t.getPkConstraintName()));
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
		String sql = String.format(
				"select cols.constraint_name, cols.table_name table_name, "
						+ "ref.table_name ref_table_name, cons.delete_rule, cols.column_name "
						+ "from all_constraints cons inner join all_cons_columns cols "
						+ "on cols.owner = cons.owner and cols.constraint_name = cons.constraint_name "
						+ "  and cols.table_name = cons.table_name "
						+ "inner join all_constraints ref on ref.owner = cons.owner "
						+ "  and ref.constraint_name = cons.r_constraint_name " + "where cons.constraint_type = 'R' "
						+ "and cons.owner = sys_context('userenv','session_user') " + "and ref.constraint_type = 'P' "
						+ "and  cons.table_name like '%s@_%%' escape '@' order by cols.constraint_name, cols.position",
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
						i.setUpdateRule(getUpdateBehaviour(conn, tableName, fkName));
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

	private FKRule getUpdateBehaviour(Connection conn, String tableName, String fkName) throws SQLException {
		// now we are looking for triggers that define update
		// rule
		String sql = String.format(
				"select trigger_name from all_triggers " + "where owner = sys_context('userenv','session_user') "
						+ "and table_name = '%s' and trigger_name in ('%s', '%s') and triggering_event = 'UPDATE'",
				tableName, getFKTriggerName(SNL, fkName), getFKTriggerName(CSC, fkName));
		Statement stmt = conn.createStatement();
		try {
			ResultSet rs = stmt.executeQuery(sql);
			if (rs.next()) {
				sql = rs.getString("TRIGGER_NAME");
				if (sql.startsWith(CSC))
					return FKRule.CASCADE;
				else if (sql.startsWith(SNL))
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
			sb = new StringBuilder(DROP_TRIGGER);
			sb.append(getFKTriggerName(SNL, fk.getConstraintName()));
			sb.append("\"");
			sql.add(sb);
			break;
		case SET_NULL:
			sb = new StringBuilder(DROP_TRIGGER);
			sb.append(getFKTriggerName(CSC, fk.getConstraintName()));
			sb.append("\"");
			sql.add(sb);
			break;
		case NO_ACTION:
		default:
			sb = new StringBuilder(DROP_TRIGGER);
			sb.append(getFKTriggerName(SNL, fk.getConstraintName()));
			sb.append("\"");
			sql.add(sb);
			sb = new StringBuilder(DROP_TRIGGER);
			sb.append(getFKTriggerName(CSC, fk.getConstraintName()));
			sb.append("\"");
			sql.add(sb);
			return;
		}

		sb = new StringBuilder();
		sb.append("create or replace trigger \"");
		if (fk.getUpdateRule() == FKRule.CASCADE) {
			sb.append(getFKTriggerName(CSC, fk.getConstraintName()));
		} else {
			sb.append(getFKTriggerName(SNL, fk.getConstraintName()));
		}
		sb.append("\" after update of ");
		Table t = fk.getReferencedTable();
		boolean needComma = false;
		for (Column c : t.getPrimaryKey().values()) {
			if (needComma)
				sb.append(", ");
			sb.append(c.getQuotedName());
			needComma = true;
		}
		sb.append(String.format(" on \"%s_%s\"", t.getGrain().getName(), t.getName()));
		sb.append(String.format(" for each row begin\n  update \"%s_%s\" set ",
				fk.getParentTable().getGrain().getName(), fk.getParentTable().getName()));

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
		String sql = String.format(DROP_TRIGGER + "%s\"", getFKTriggerName(SNL, fkName));
		sqlQueue.add(sql);
		sql = String.format(DROP_TRIGGER + "%s\"", getFKTriggerName(CSC, fkName));
		sqlQueue.add(sql);
	}

	@Override
	String getLimitedSQL(GrainElement t, String whereClause, String orderBy, long offset, long rowCount) {
		if (offset == 0 && rowCount == 0)
			throw new IllegalArgumentException();
		String sql;
		if (offset == 0) {
			// No offset -- simpler query
			sql = String.format("with a as (%s) select a.* from a where rownum <= %d",
					getSelectFromOrderBy(t, whereClause, orderBy), rowCount);
		} else if (rowCount == 0) {
			// No rowCount -- simpler query
			sql = String.format(
					"with a as (%s) select * from (select a.*, ROWNUM rnum " + "from a) where rnum >= %d order by rnum",
					getSelectFromOrderBy(t, whereClause, orderBy), offset + 1L);

		} else {
			sql = String.format(
					"with a as (%s) select * from (select a.*, ROWNUM rnum "
							+ "from a where rownum <= %d) where rnum >= %d order by rnum",
					getSelectFromOrderBy(t, whereClause, orderBy), offset + rowCount, offset + 1L);
		}
		return sql;
	}

	@Override
	Map<String, DBIndexInfo> getIndices(Connection conn, Grain g) throws CelestaException {
		String sql = String
				.format("select ind.table_name TABLE_NAME, ind.index_name INDEX_NAME, cols.column_name COLUMN_NAME,"
						+ " cols.column_position POSITION " + "from all_indexes ind "
						+ "inner join all_ind_columns cols " + "on ind.owner = cols.index_owner "
						+ "and ind.table_name = cols.table_name " + "and ind.index_name = cols.index_name "
						+ "where ind.owner = sys_context('userenv','session_user') and ind.uniqueness = 'NONUNIQUE' "
						+ "and ind.table_name like '%s@_%%' escape '@'"
						+ "order by ind.table_name, ind.index_name, cols.column_position", g.getName());

		// System.out.println(sql);

		Map<String, DBIndexInfo> result = new HashMap<>();
		try {
			Statement stmt = conn.createStatement();
			try {
				DBIndexInfo i = null;
				ResultSet rs = stmt.executeQuery(sql);
				while (rs.next()) {
					String tabName = rs.getString("TABLE_NAME");
					Matcher m = TABLE_PATTERN.matcher(tabName);
					m.find();
					tabName = m.group(2);
					String indName = rs.getString("INDEX_NAME");
					m = TABLE_PATTERN.matcher(indName);
					if (m.find()) {
						indName = m.group(2);
					} else {
						/*
						 * Если название индекса не соответствует ожидаемому
						 * шаблону, то это -- индекс, добавленный вне Celesta и
						 * его следует удалить. Мы добавляем знаки ## перед
						 * именем индекса. Далее система, не найдя индекс с
						 * такими метаданными, поставит такой индекс на
						 * удаление. Метод удаления, обнаружив ## в начале имени
						 * индекса, удалит их.
						 */
						indName = "##" + indName;
					}

					if (i == null || !i.getTableName().equals(tabName) || !i.getIndexName().equals(indName)) {
						i = new DBIndexInfo(tabName, indName);
						result.put(indName, i);
					}
					i.getColumnNames().add(rs.getString("COLUMN_NAME"));
				}
			} finally {
				stmt.close();
			}
		} catch (SQLException e) {
			throw new CelestaException("Could not get indices information: %s", e.getMessage());
		}
		return result;
	}

	@Override
	public List<String> getViewList(Connection conn, Grain g) throws CelestaException {

		String sql = String.format(
				"select view_name from all_views "
						+ "where owner = sys_context('userenv','session_user') and view_name like '%s@_%%' escape '@'",
				g.getName());
		List<String> result = new LinkedList<>();
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				String buf = rs.getString(1);
				Matcher m = TABLE_PATTERN.matcher(buf);
				m.find();
				result.add(m.group(2));
			}
		} catch (SQLException e) {
			throw new CelestaException("Cannot get views list: %s", e.toString());
		}
		return result;
	}

	@Override
	public SQLGenerator getViewSQLGenerator() {
		return new SQLGenerator() {

			@Override
			protected String viewName(View v) {
				return String.format(TABLE_TEMPLATE, v.getGrain().getName(), v.getName());
			}

			@Override
			protected String tableName(TableRef tRef) {
				Table t = tRef.getTable();
				return String.format(TABLE_TEMPLATE + " \"%s\"", t.getGrain().getName(), t.getName(), tRef.getAlias());
			}

			@Override
			protected String checkForDate(String lexValue) {
				try {
					return translateDate(lexValue);
				} catch (CelestaException e) {
					// This is not a date
					return lexValue;
				}
			}

		};
	}

	private static String getUpdTriggerName(Table table) {
		String result = String.format("%s_%s_upd", table.getGrain().getName(), table.getName());
		result = NamedElement.limitName(result);
		return result;
	}

	private void dropVersioningTrigger(Connection conn, Table t) throws CelestaException {
		// First of all, we are about to check if trigger exists
		String triggerName = getUpdTriggerName(t);
		String sql = String.format(
				SELECT_TRIGGER_BODY
						+ "and table_name = '%s_%s' and trigger_name = '%s' and triggering_event = 'UPDATE'",
				t.getGrain().getName(), t.getName(), triggerName);
		try {
			Statement stmt = conn.createStatement();
			try {
				boolean triggerExists = false;

				ResultSet rs = stmt.executeQuery(sql);
				triggerExists = rs.next();
				rs.close();

				if (triggerExists) {
					// DROP TRIGGER
					sql = String.format(DROP_TRIGGER + "%s\"", triggerName);
					stmt.executeUpdate(sql);
				} else {
					return;
				}

			} finally {
				stmt.close();
			}
		} catch (SQLException e) {
			throw new CelestaException("Could not update version check trigger on %s.%s: %s", t.getGrain().getName(),
					t.getName(), e.getMessage());
		}
	}

	@Override
	public void updateVersioningTrigger(Connection conn, Table t) throws CelestaException {
		// First of all, we are about to check if trigger exists
		String triggerName = getUpdTriggerName(t);
		String sql = String.format(
				SELECT_TRIGGER_BODY
						+ "and table_name = '%s_%s' and trigger_name = '%s' and triggering_event = 'UPDATE'",
				t.getGrain().getName(), t.getName(), triggerName);
		try {
			Statement stmt = conn.createStatement();
			try {
				boolean triggerExists = false;

				ResultSet rs = stmt.executeQuery(sql);
				triggerExists = rs.next();
				rs.close();

				if (t.isVersioned()) {
					if (triggerExists) {
						return;
					} else {
						// CREATE TRIGGER
						sql = String.format("CREATE OR REPLACE TRIGGER \"%s\" BEFORE UPDATE ON \"%s_%s\" FOR EACH ROW\n"
								+ "BEGIN\n" + "  IF :new.\"recversion\" <> :old.\"recversion\" THEN\n"
								+ "    raise_application_error( -20001, 'record version check failure' );\n"
								+ "  END IF;\n" + "  :new.\"recversion\" := :new.\"recversion\" + 1;\n" + "END;",
								triggerName, t.getGrain().getName(), t.getName());
						// System.out.println(sql);
						stmt.executeUpdate(sql);
					}
				} else {
					if (triggerExists) {
						// DROP TRIGGER
						sql = String.format(DROP_TRIGGER + "%s\"", triggerName);
						stmt.executeUpdate(sql);
					} else {
						return;
					}
				}
			} finally {
				stmt.close();
			}
		} catch (SQLException e) {
			throw new CelestaException("Could not update version check trigger on %s.%s: %s", t.getGrain().getName(),
					t.getName(), e.getMessage());
		}

	}

	@Override
	// CHECKSTYLE:OFF 6 params
	PreparedStatement getNavigationStatement(Connection conn, GrainElement t, Map<String, AbstractFilter> filters,
			Expr complexFilter, String orderBy, String navigationWhereClause) throws CelestaException {
		// CHECKSTYLE:ON
		if (navigationWhereClause == null)
			throw new IllegalArgumentException();

		StringBuilder w = new StringBuilder();
		w.append(getWhereClause(t, filters, complexFilter));
		if (w.length() > 0 && navigationWhereClause.length() > 0)
			w.append(" and ");
		w.append(navigationWhereClause);
		if (w.length() > 0)
			w.append(" and ");
		w.append("rownum = 1");

		if (orderBy.length() > 0)
			w.append(" order by " + orderBy);
		String sql = String.format(SELECT_S_FROM + tableTemplate() + "  %s", getTableFieldsListExceptBLOBs(t),
				t.getGrain().getName(), t.getName(), "where " + w);
		// System.out.println(sql);
		return prepareStatement(conn, sql);
	}

	@Override
	public String translateDate(String date) throws CelestaException {
		try {
			Date d = DateTimeColumn.parseISODate(date);
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			return String.format("date '%s'", df.format(d));
		} catch (ParseException e) {
			throw new CelestaException(e.getMessage());
		}

	}

	@Override
	public void resetIdentity(Connection conn, Table t, int i) throws SQLException {
		String sequenceName = getSequenceName(t);
		Statement stmt = conn.createStatement();
		try {
			String sql = String.format("select \"%s\".nextval from dual", sequenceName);
			ResultSet rs = stmt.executeQuery(sql);
			rs.next();
			int curVal = rs.getInt(1);
			rs.close();
			sql = String.format("alter sequence \"%s\" increment by %d minvalue 1", sequenceName, i - curVal - 1);
			stmt.executeUpdate(sql);
			sql = String.format("select \"%s\".nextval from dual", sequenceName);
			stmt.executeQuery(sql).close();
			sql = String.format("alter sequence \"%s\" increment by 1 minvalue 1", sequenceName);
			stmt.executeUpdate(sql);
		} finally {
			stmt.close();
		}
	}

	@Override
	public int getDBPid(Connection conn) throws CelestaException {
		try {
			Statement stmt = conn.createStatement();
			try {
				ResultSet rs = stmt.executeQuery("select sys_context('userenv','sessionid') from dual");
				if (rs.next()) {
					return rs.getInt(1);
				} else {
					return 0;
				}
			} finally {
				stmt.close();
			}
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
	}
}

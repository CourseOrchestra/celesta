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

package ru.curs.celesta.dbutils.adaptors;

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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.meta.DBColumnInfo;
import ru.curs.celesta.dbutils.meta.DBFKInfo;
import ru.curs.celesta.dbutils.meta.DBIndexInfo;
import ru.curs.celesta.dbutils.meta.DBPKInfo;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;
import ru.curs.celesta.score.BinaryColumn;
import ru.curs.celesta.score.BooleanColumn;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.DateTimeColumn;
import ru.curs.celesta.score.FloatingColumn;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.GrainElement;
import ru.curs.celesta.score.Index;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.SQLGenerator;
import ru.curs.celesta.score.StringColumn;
import ru.curs.celesta.score.Table;
import ru.curs.celesta.score.View;

/**
 * Адаптер MSSQL.
 * 
 */
final class MSSQLAdaptor extends DBAdaptor {

	private static final String SELECT_TOP_1 = "select top 1 %s from ";
	private static final String WHERE_S = " where %s;";
	private static final int DOUBLE_PRECISION = 53;

	/**
	 * Определитель колонок для MSSQL.
	 */
	abstract static class MSColumnDefiner extends ColumnDefiner {
		abstract String getLightDefaultDefinition(Column c);
	}

	private static final Map<Class<? extends Column>, MSColumnDefiner> TYPES_DICT = new HashMap<>();

	static {
		TYPES_DICT.put(IntegerColumn.class, new MSColumnDefiner() {
			@Override
			String dbFieldType() {
				return "int";
			}

			@Override
			String getMainDefinition(Column c) {
				return join(c.getQuotedName(), dbFieldType(), nullable(c));
			}

			@Override
			String getDefaultDefinition(Column c) {
				IntegerColumn ic = (IntegerColumn) c;
				if (ic.getDefaultValue() != null)
					return msSQLDefault(c) + ic.getDefaultValue();
				return "";
			}

			@Override
			String getLightDefaultDefinition(Column c) {
				IntegerColumn ic = (IntegerColumn) c;
				if (ic.getDefaultValue() != null)
					return DEFAULT + ic.getDefaultValue();
				return "";
			}
		});

		TYPES_DICT.put(FloatingColumn.class, new MSColumnDefiner() {

			@Override
			String dbFieldType() {
				return "float(" + DOUBLE_PRECISION + ")";
			}

			@Override
			String getMainDefinition(Column c) {
				return join(c.getQuotedName(), dbFieldType(), nullable(c));
			}

			@Override
			String getDefaultDefinition(Column c) {
				FloatingColumn ic = (FloatingColumn) c;
				String defaultStr = "";
				if (ic.getDefaultValue() != null) {
					defaultStr = msSQLDefault(c) + ic.getDefaultValue();
				}
				return defaultStr;
			}

			@Override
			String getLightDefaultDefinition(Column c) {
				FloatingColumn ic = (FloatingColumn) c;
				String defaultStr = "";
				if (ic.getDefaultValue() != null) {
					defaultStr = DEFAULT + ic.getDefaultValue();
				}
				return defaultStr;
			}
		});

		TYPES_DICT.put(StringColumn.class, new MSColumnDefiner() {

			@Override
			String dbFieldType() {
				return "nvarchar";
			}

			@Override
			String getMainDefinition(Column c) {
				StringColumn ic = (StringColumn) c;
				String fieldType = String.format("%s(%s)", dbFieldType(), ic.isMax() ? "max" : ic.getLength());
				return join(c.getQuotedName(), fieldType, nullable(c));
			}

			@Override
			String getDefaultDefinition(Column c) {
				StringColumn ic = (StringColumn) c;
				String defaultStr = "";
				if (ic.getDefaultValue() != null) {
					defaultStr = msSQLDefault(c) + StringColumn.quoteString(ic.getDefaultValue());
				}
				return defaultStr;
			}

			@Override
			String getLightDefaultDefinition(Column c) {
				StringColumn ic = (StringColumn) c;
				String defaultStr = "";
				if (ic.getDefaultValue() != null) {
					defaultStr = DEFAULT + StringColumn.quoteString(ic.getDefaultValue());
				}
				return defaultStr;
			}
		});

		TYPES_DICT.put(BinaryColumn.class, new MSColumnDefiner() {

			@Override
			String dbFieldType() {
				return "varbinary(max)";
			}

			@Override
			String getMainDefinition(Column c) {
				return join(c.getQuotedName(), dbFieldType(), nullable(c));
			}

			@Override
			String getDefaultDefinition(Column c) {
				BinaryColumn ic = (BinaryColumn) c;

				String defaultStr = "";
				if (ic.getDefaultValue() != null) {
					defaultStr = msSQLDefault(c) + ic.getDefaultValue();
				}
				return defaultStr;

			}

			@Override
			String getLightDefaultDefinition(Column c) {
				BinaryColumn ic = (BinaryColumn) c;

				String defaultStr = "";
				if (ic.getDefaultValue() != null) {
					defaultStr = DEFAULT + ic.getDefaultValue();
				}
				return defaultStr;
			}
		});

		TYPES_DICT.put(DateTimeColumn.class, new MSColumnDefiner() {

			@Override
			String dbFieldType() {
				return "datetime";
			}

			@Override
			String getMainDefinition(Column c) {
				return join(c.getQuotedName(), dbFieldType(), nullable(c));
			}

			@Override
			String getDefaultDefinition(Column c) {
				DateTimeColumn ic = (DateTimeColumn) c;
				String defaultStr = "";
				if (ic.isGetdate()) {
					defaultStr = msSQLDefault(c) + "getdate()";
				} else if (ic.getDefaultValue() != null) {
					DateFormat df = new SimpleDateFormat("yyyyMMdd");
					defaultStr = String.format(msSQLDefault(c) + " '%s'", df.format(ic.getDefaultValue()));
				}
				return defaultStr;
			}

			@Override
			String getLightDefaultDefinition(Column c) {
				DateTimeColumn ic = (DateTimeColumn) c;
				String defaultStr = "";
				if (ic.isGetdate()) {
					defaultStr = DEFAULT + "getdate()";
				} else if (ic.getDefaultValue() != null) {
					DateFormat df = new SimpleDateFormat("yyyyMMdd");
					defaultStr = String.format(DEFAULT + " '%s'", df.format(ic.getDefaultValue()));
				}
				return defaultStr;
			}
		});

		TYPES_DICT.put(BooleanColumn.class, new MSColumnDefiner() {

			@Override
			String dbFieldType() {
				return "bit";
			}

			@Override
			String getMainDefinition(Column c) {
				return join(c.getQuotedName(), dbFieldType(), nullable(c));
			}

			@Override
			String getDefaultDefinition(Column c) {
				BooleanColumn ic = (BooleanColumn) c;
				String defaultStr = "";
				if (ic.getDefaultValue() != null) {
					defaultStr = msSQLDefault(c) + "'" + ic.getDefaultValue() + "'";
				}
				return defaultStr;
			}

			@Override
			String getLightDefaultDefinition(Column c) {
				BooleanColumn ic = (BooleanColumn) c;
				String defaultStr = "";
				if (ic.getDefaultValue() != null) {
					defaultStr = DEFAULT + "'" + ic.getDefaultValue() + "'";
				}
				return defaultStr;
			}
		});
	}

	private static String msSQLDefault(Column c) {
		return String.format("constraint \"def_%s_%s\" ", c.getParentTable().getName(), c.getName())
				+ ColumnDefiner.DEFAULT;
	}

	@Override
	public boolean tableExists(Connection conn, String schema, String name) throws CelestaException {
		String sql = String.format("select coalesce(object_id('%s.%s'), -1)", schema, name);
		try {
			Statement check = conn.createStatement();
			ResultSet rs = check.executeQuery(sql);
			try {
				rs.next();
				return rs.getInt(1) != -1;
			} finally {
				check.close();
			}
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
	}

	@Override
	boolean userTablesExist(Connection conn) throws SQLException {
		PreparedStatement check = conn.prepareStatement("select count(*) from sys.tables;");
		ResultSet rs = check.executeQuery();
		try {
			rs.next();
			return rs.getInt(1) != 0;
		} finally {
			rs.close();
			check.close();
		}
	}

	@Override
	void createSchemaIfNotExists(Connection conn, String name) throws SQLException {
		PreparedStatement check = conn.prepareStatement(String.format("select coalesce(SCHEMA_ID('%s'), -1)", name));
		ResultSet rs = check.executeQuery();
		try {
			rs.next();
			if (rs.getInt(1) == -1) {
				PreparedStatement create = conn.prepareStatement(String.format("create schema \"%s\";", name));
				create.execute();
				create.close();
			}
		} finally {
			rs.close();
			check.close();
		}
	}

	@Override
	MSColumnDefiner getColumnDefiner(Column c) {
		return TYPES_DICT.get(c.getClass());
	}

	@Override
	public PreparedStatement getOneFieldStatement(Connection conn, Column c, String where) throws CelestaException {
		Table t = c.getParentTable();
		String sql = String.format(SELECT_TOP_1 + tableTemplate() + WHERE_S, c.getQuotedName(), t.getGrain().getName(),
				t.getName(), where);
		return prepareStatement(conn, sql);
	}

	@Override
	public PreparedStatement getOneRecordStatement(Connection conn, Table t, String where) throws CelestaException {
		String sql = String.format(SELECT_TOP_1 + tableTemplate() + WHERE_S, getTableFieldsListExceptBLOBs(t),
				t.getGrain().getName(), t.getName(), where);
		return prepareStatement(conn, sql);
	}

	@Override
	public PreparedStatement getInsertRecordStatement(Connection conn, Table t, boolean[] nullsMask,
			List<ParameterSetter> program) throws CelestaException {

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
			program.add(ParameterSetter.create(i));
		}

		final String sql;

		if (fields.length() == 0 && params.length() == 0) {
			sql = String.format("insert into " + tableTemplate() + " default values;", t.getGrain().getName(),
					t.getName());
		} else {
			sql = String.format("insert " + tableTemplate() + " (%s) values (%s);", t.getGrain().getName(),
					t.getName(), fields.toString(), params.toString());
		}

		return prepareStatement(conn, sql);
	}

	@Override
	public PreparedStatement getDeleteRecordStatement(Connection conn, Table t, String where) throws CelestaException {
		String sql = String.format("delete " + tableTemplate() + WHERE_S, t.getGrain().getName(), t.getName(), where);
		return prepareStatement(conn, sql);
	}

	@Override
	public PreparedStatement deleteRecordSetStatement(Connection conn, Table t, String where) throws CelestaException {
		// Готовим запрос на удаление
		String sql = String.format("delete " + tableTemplate() + " %s;", t.getGrain().getName(), t.getName(),
				where.isEmpty() ? "" : "where " + where);
		try {
			PreparedStatement result = conn.prepareStatement(sql);
			return result;
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
	}

	@Override
	public int getCurrentIdent(Connection conn, Table t) throws CelestaException {
		String sql = String.format("select seqvalue from celesta.sequences where grainid = '%s' and tablename = '%s'",
				t.getGrain().getName(), t.getName());
		try {
			Statement stmt = conn.createStatement();
			try {
				ResultSet rs = stmt.executeQuery(sql);
				if (!rs.next())
					throw new CelestaException("Celesta sequense for %s.%s is not initialized.", t.getGrain().getName(),
							t.getName());
				return rs.getInt(1);
			} finally {
				stmt.close();
			}
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
	}

	@Override
	public String getInFilterClause(Table table, Table otherTable, List<String> fields, List<String> otherFields) {
		String template = "( %s ) IN (SELECT %s FROM %s )";

		String tableStr = String.format(tableTemplate(), table.getGrain().getName(), table.getName());
		String otherTableStr = String.format(tableTemplate(), otherTable.getGrain().getName(), otherTable.getName());

		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < fields.size(); ++i) {
			sb.append(tableStr).append(".\"").append(fields.get(i)).append("\"")
					.append(" = ")
					.append(otherTableStr).append(".\"").append(otherFields.get(i)).append("\"");

			if (i + 1 != fields.size()) {
				sb.append(" AND ");
			}
		}

		String result = String.format(template, otherTableStr, sb.toString());
		return result;
	}

	@Override
	String[] getCreateIndexSQL(Index index) {
		String fieldList = getFieldList(index.getColumns().keySet());
		String sql = String.format("CREATE INDEX %s ON " + tableTemplate() + " (%s)", index.getQuotedName(),
				index.getTable().getGrain().getName(), index.getTable().getName(), fieldList);
		String[] result = { sql };
		return result;

	}

	@Override
	String[] getDropIndexSQL(Grain g, DBIndexInfo dBIndexInfo) {
		String sql = String.format("DROP INDEX %s ON " + tableTemplate(), dBIndexInfo.getIndexName(), g.getName(),
				dBIndexInfo.getTableName());
		String[] result = { sql };
		return result;
	}

	private boolean checkIfVarcharMax(Connection conn, Column c) throws SQLException {
		PreparedStatement checkForMax = conn.prepareStatement(String.format(
				"select max_length from sys.columns where " + "object_id  = OBJECT_ID('%s.%s') and name = '%s'",
				c.getParentTable().getGrain().getName(), c.getParentTable().getName(), c.getName()));
		try {
			ResultSet rs = checkForMax.executeQuery();
			if (rs.next()) {
				int len = rs.getInt(1);
				return len == -1;
			}
		} finally {
			checkForMax.close();
		}
		return false;

	}

	private boolean checkForIncrementTrigger(Connection conn, Column c) throws SQLException {
		PreparedStatement checkForTrigger = conn.prepareStatement(
				String.format("select text from sys.syscomments where id = object_id('%s.\"%s_inc\"')",
						c.getParentTable().getGrain().getQuotedName(), c.getParentTable().getName()));
		try {
			ResultSet rs = checkForTrigger.executeQuery();
			if (rs.next()) {
				String body = rs.getString(1);
				if (body != null && body.contains(String.format("/*IDENTITY %s*/", c.getName())))
					return true;
			}
		} finally {
			checkForTrigger.close();
		}
		return false;
	}

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
	// CHECKSTYLE:OFF
	@SuppressWarnings("unchecked")
	@Override
	public DBColumnInfo getColumnInfo(Connection conn, Column c) throws CelestaException {
		// CHECKSTYLE:ON
		try {
			DatabaseMetaData metaData = conn.getMetaData();
			ResultSet rs = metaData.getColumns(null, c.getParentTable().getGrain().getName(),
					c.getParentTable().getName(), c.getName());
			try {
				if (rs.next()) {
					DBColumnInfo result = new DBColumnInfo();
					result.setName(rs.getString(COLUMN_NAME));
					String typeName = rs.getString("TYPE_NAME");
					if ("varbinary".equalsIgnoreCase(typeName) && checkIfVarcharMax(conn, c)) {
						result.setType(BinaryColumn.class);
					} else if ("int".equalsIgnoreCase(typeName)) {
						result.setType(IntegerColumn.class);
						result.setIdentity(checkForIncrementTrigger(conn, c));
					} else if ("float".equalsIgnoreCase(typeName) && rs.getInt("COLUMN_SIZE") == DOUBLE_PRECISION) {
						result.setType(FloatingColumn.class);
					} else {
						for (Class<?> cc : COLUMN_CLASSES)
							if (TYPES_DICT.get(cc).dbFieldType().equalsIgnoreCase(typeName)) {
								result.setType((Class<? extends Column>) cc);
								break;
							}
					}
					result.setNullable(rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls);
					if (result.getType() == StringColumn.class) {
						result.setLength(rs.getInt("COLUMN_SIZE"));
						result.setMax(checkIfVarcharMax(conn, c));
					}
					String defaultBody = rs.getString("COLUMN_DEF");
					if (defaultBody != null) {
						int i = 0;
						// Снимаем наружные скобки
						while (defaultBody.charAt(i) == '('
								&& defaultBody.charAt(defaultBody.length() - i - 1) == ')') {
							i++;
						}
						defaultBody = defaultBody.substring(i, defaultBody.length() - i);
						if (BooleanColumn.class == result.getType() || DateTimeColumn.class == result.getType())
							defaultBody = defaultBody.toUpperCase();
						result.setDefaultValue(defaultBody);
					}
					return result;
				} else {
					return null;
				}
			} finally {
				rs.close();
			}
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}

	}

	@Override
	public void updateColumn(Connection conn, Column c, DBColumnInfo actual) throws CelestaException {

		String sql;
		if (!"".equals(actual.getDefaultValue())) {
			sql = String.format(ALTER_TABLE + tableTemplate() + " drop constraint \"def_%s_%s\"",
					c.getParentTable().getGrain().getName(), c.getParentTable().getName(), c.getParentTable().getName(),
					c.getName());
			runUpdateColumnSQL(conn, c, sql);
		}

		String def = getColumnDefiner(c).getMainDefinition(c);
		sql = String.format(ALTER_TABLE + tableTemplate() + " alter column %s", c.getParentTable().getGrain().getName(),
				c.getParentTable().getName(), def);
		runUpdateColumnSQL(conn, c, sql);

		def = getColumnDefiner(c).getDefaultDefinition(c);
		if (!"".equals(def)) {
			sql = String.format(ALTER_TABLE + tableTemplate() + " add %s for %s",
					c.getParentTable().getGrain().getName(), c.getParentTable().getName(), def, c.getQuotedName());
			runUpdateColumnSQL(conn, c, sql);
		}
	}

	@Override
	public void manageAutoIncrement(Connection conn, Table t) throws SQLException {
		// 1. Firstly, we have to clean up table from any auto-increment
		// triggers
		String triggerName = String.format("\"%s\".\"%s_inc\"", t.getGrain().getName(), t.getName());
		String sql = String.format("drop trigger %s", triggerName);
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

		// 3. Now, we know that we surely have IDENTITY field, and we must
		// assure that we have an appropriate sequence.
		sql = String.format("insert into celesta.sequences (grainid, tablename) values ('%s', '%s')",
				t.getGrain().getName(), t.getName());
		stmt = conn.prepareStatement(sql);
		try {
			stmt.executeUpdate();
		} catch (SQLException e) {
			// do nothing
			sql = "";
		} finally {
			stmt.close();
		}

		// 4. Now we have to create the auto-increment trigger
		StringBuilder body = new StringBuilder();
		body.append(String.format("create trigger %s on %s.%s instead of insert as begin\n", triggerName,
				t.getGrain().getQuotedName(), t.getQuotedName()));
		body.append(String.format("  /*IDENTITY %s*/\n", ic.getName()));
		body.append("  set nocount on;\n");
		body.append("  begin transaction;\n");
		body.append("  declare @id int;\n");
		body.append("  declare @idt table (id int);\n");
		body.append("  declare @tmp table (\n");

		StringBuilder selectList = new StringBuilder();
		StringBuilder insertList = new StringBuilder();
		StringBuilder fullList = new StringBuilder();
		Iterator<Column> i = t.getColumns().values().iterator();
		while (i.hasNext()) {
			Column c = i.next();
			padComma(fullList);
			fullList.append(c.getQuotedName());

			MSColumnDefiner d = getColumnDefiner(c);
			body.append("    ");
			if (c == ic) {
				body.append(c.getName());
				body.append(" int not null identity");
				padComma(insertList);
				insertList.append("@id + ");
				insertList.append(c.getQuotedName());
			} else {
				body.append(ColumnDefiner.join(d.getMainDefinition(c), d.getLightDefaultDefinition(c)));
				padComma(selectList);
				padComma(insertList);
				selectList.append(c.getQuotedName());
				insertList.append(c.getQuotedName());
			}
			body.append(i.hasNext() ? ",\n" : "\n");
		}
		body.append("  );\n");
		body.append(String.format("  insert into @tmp (%s) select %s from inserted;\n", selectList, selectList));

		body.append(String.format(
				"  update celesta.sequences set seqvalue = seqvalue + @@IDENTITY "
						+ "output deleted.seqvalue into @idt where grainid = '%s' and tablename = '%s';\n",
				t.getGrain().getName(), t.getName()));
		body.append("  select @id = id from @idt;\n");
		body.append(String.format("  insert into %s.%s (%s) select %s from @tmp;\n", t.getGrain().getQuotedName(),
				t.getQuotedName(), fullList, insertList));
		body.append("  commit transaction;\n");
		body.append("end;\n");

		// System.out.println(body.toString());

		stmt = conn.prepareStatement(body.toString());
		try {
			stmt.executeUpdate();
		} finally {
			stmt.close();
		}
	}

	@Override
	void dropAutoIncrement(Connection conn, Table t) throws SQLException {
		String sql = String.format("delete from celesta.sequences where grainid = '%s' and tablename = '%s';\n",
				t.getGrain().getName(), t.getName());
		PreparedStatement stmt = conn.prepareStatement(sql);
		try {
			stmt.executeUpdate();
		} catch (SQLException e) {
			// do nothing
			sql = "";
		} finally {
			stmt.close();
		}
	}

	@Override
	public DBPKInfo getPKInfo(Connection conn, Table t) throws CelestaException {

		DBPKInfo result = new DBPKInfo();
		try {
			String sql = String.format(
					"select cons.CONSTRAINT_NAME, cols.COLUMN_NAME from INFORMATION_SCHEMA.KEY_COLUMN_USAGE cols "
							+ "inner join INFORMATION_SCHEMA.TABLE_CONSTRAINTS cons "
							+ "on cols.TABLE_SCHEMA = cons.TABLE_SCHEMA " + "and cols.TABLE_NAME = cons.TABLE_NAME "
							+ "and cols.CONSTRAINT_NAME = cons.CONSTRAINT_NAME "
							+ "where cons.CONSTRAINT_TYPE = 'PRIMARY KEY' and cons.TABLE_SCHEMA = '%s' "
							+ "and cons.TABLE_NAME = '%s' order by ORDINAL_POSITION",
					t.getGrain().getName(), t.getName());
			// System.out.println(sql);
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
	public void dropPK(Connection conn, Table t, String pkName) throws CelestaException {
		String sql = String.format("alter table %s.%s drop constraint \"%s\"", t.getGrain().getQuotedName(),
				t.getQuotedName(), pkName);
		try {
			Statement stmt = conn.createStatement();
			try {
				stmt.executeUpdate(sql);
			} finally {
				stmt.close();
			}
		} catch (SQLException e) {
			throw new CelestaException("Cannot drop PK '%s': %s", pkName, e.getMessage());
		}
	}

	@Override
	public void createPK(Connection conn, Table t) throws CelestaException {
		StringBuilder sql = new StringBuilder();
		sql.append(String.format("alter table %s.%s add constraint \"%s\" " + " primary key (",
				t.getGrain().getQuotedName(), t.getQuotedName(), t.getPkConstraintName()));
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
			throw new CelestaException("Cannot create PK '%s': %s", t.getPkConstraintName(), e.getMessage());
		}

	}

	@Override
	public List<DBFKInfo> getFKInfo(Connection conn, Grain g) throws CelestaException {
		// Full foreign key information query
		String sql = String.format(
				"SELECT RC.CONSTRAINT_SCHEMA AS 'GRAIN'" + "   , KCU1.CONSTRAINT_NAME AS 'FK_CONSTRAINT_NAME'"
						+ "   , KCU1.TABLE_NAME AS 'FK_TABLE_NAME'" + "   , KCU1.COLUMN_NAME AS 'FK_COLUMN_NAME'"
						+ "   , KCU2.TABLE_SCHEMA AS 'REF_GRAIN'" + "   , KCU2.TABLE_NAME AS 'REF_TABLE_NAME'"
						+ "   , RC.UPDATE_RULE, RC.DELETE_RULE " + "FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC "
						+ "INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1 "
						+ "   ON  KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG"
						+ "   AND KCU1.CONSTRAINT_SCHEMA  = RC.CONSTRAINT_SCHEMA"
						+ "   AND KCU1.CONSTRAINT_NAME    = RC.CONSTRAINT_NAME "
						+ "INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU2"
						+ "   ON  KCU2.CONSTRAINT_CATALOG = RC.UNIQUE_CONSTRAINT_CATALOG"
						+ "   AND KCU2.CONSTRAINT_SCHEMA  = RC.UNIQUE_CONSTRAINT_SCHEMA"
						+ "   AND KCU2.CONSTRAINT_NAME    = RC.UNIQUE_CONSTRAINT_NAME"
						+ "   AND KCU2.ORDINAL_POSITION   = KCU1.ORDINAL_POSITION "
						+ "WHERE RC.CONSTRAINT_SCHEMA = '%s' " + "ORDER BY KCU1.CONSTRAINT_NAME, KCU1.ORDINAL_POSITION",
				g.getName());

		// System.out.println(sql);

		List<DBFKInfo> result = new LinkedList<>();
		try {
			Statement stmt = conn.createStatement();
			try {
				DBFKInfo i = null;
				ResultSet rs = stmt.executeQuery(sql);
				while (rs.next()) {
					String fkName = rs.getString("FK_CONSTRAINT_NAME");
					if (i == null || !i.getName().equals(fkName)) {
						i = new DBFKInfo(fkName);
						result.add(i);
						i.setTableName(rs.getString("FK_TABLE_NAME"));
						i.setRefGrainName(rs.getString("REF_GRAIN"));
						i.setRefTableName(rs.getString("REF_TABLE_NAME"));
						i.setUpdateRule(getFKRule(rs.getString("UPDATE_RULE")));
						i.setDeleteRule(getFKRule(rs.getString("DELETE_RULE")));
					}
					i.getColumnNames().add(rs.getString("FK_COLUMN_NAME"));
				}
			} finally {
				stmt.close();
			}
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
		return result;
	}

	@Override
	String getLimitedSQL(GrainElement t, String whereClause, String orderBy, long offset, long rowCount) {
		if (offset == 0 && rowCount == 0)
			throw new IllegalArgumentException();
		String sql;
		String sqlwhere = "".equals(whereClause) ? "" : " where " + whereClause;
		String rowFilter;
		if (offset == 0) {
			// Запрос только с ограничением числа записей -- применяем MS SQL
			// Server TOP-конструкцию.
			sql = String.format("select top %d %s from %s.%s", rowCount, getTableFieldsListExceptBLOBs(t),
					t.getGrain().getName(), t.getName()) + sqlwhere + " order by " + orderBy;
			return sql;
			// Иначе -- запрос с пропуском начальных записей -- применяем
			// ROW_NUMBER
		} else if (rowCount == 0) {
			rowFilter = String.format(">= %d", offset + 1L);
		} else {
			rowFilter = String.format("between %d and %d", offset + 1L, offset + rowCount);
		}
		sql = String.format(
				"with a as " + "(select ROW_NUMBER() OVER (ORDER BY %s) as [limit_row_number], %s from %s.%s %s) "
						+ " select * from a where [limit_row_number] %s",
				orderBy, getTableFieldsListExceptBLOBs(t), t.getGrain().getName(), t.getName(), sqlwhere, rowFilter);
		return sql;
	}

	@Override
	public Map<String, DBIndexInfo> getIndices(Connection conn, Grain g) throws CelestaException {
		String sql = String.format("select " + "    s.name as SchemaName," + "    o.name as TableName,"
				+ "    i.name as IndexName," + "    co.name as ColumnName," + "    ic.key_ordinal as ColumnOrder "
				+ "from sys.indexes i " + "inner join sys.objects o on i.object_id = o.object_id "
				+ "inner join sys.index_columns ic on ic.object_id = i.object_id " + "    and ic.index_id = i.index_id "
				+ "inner join sys.columns co on co.object_id = i.object_id " + "    and co.column_id = ic.column_id "
				+ "inner join sys.schemas s on o.schema_id = s.schema_id "
				+ "where i.is_primary_key = 0 and o.[type] = 'U' " + " and s.name = '%s' "
				+ " order by o.name,  i.[name], ic.key_ordinal;", g.getName());

		// System.out.println(sql);

		Map<String, DBIndexInfo> result = new HashMap<>();
		try {
			Statement stmt = conn.createStatement();
			try {
				DBIndexInfo i = null;
				ResultSet rs = stmt.executeQuery(sql);
				while (rs.next()) {
					String tabName = rs.getString("TableName");
					String indName = rs.getString("IndexName");
					if (i == null || !i.getTableName().equals(tabName) || !i.getIndexName().equals(indName)) {
						i = new DBIndexInfo(tabName, indName);
						result.put(indName, i);
					}
					i.getColumnNames().add(rs.getString("ColumnName"));
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
	public SQLGenerator getViewSQLGenerator() {
		return new SQLGenerator() {

			@Override
			protected String concat() {
				return " + ";
			}

			@Override
			protected String preamble(View view) {
				return String.format("create view %s as", viewName(view));
			}

			@Override
			protected String boolLiteral(boolean val) {
				return val ? "1" : "0";
			}
		};
	}

	@Override
	public void updateVersioningTrigger(Connection conn, Table t) throws CelestaException {

		// First of all, we are about to check if trigger exists
		String sql = String.format(
				"SELECT COUNT(*) FROM sys.triggers tr " + "INNER JOIN sys.tables t ON tr.parent_id = t.object_id "
						+ "WHERE t.schema_id = SCHEMA_ID('%s') and tr.name = '%s_upd'",
				t.getGrain().getName(), t.getName());
		try {
			Statement stmt = conn.createStatement();
			try {
				ResultSet rs = stmt.executeQuery(sql);
				rs.next();
				boolean triggerExists = rs.getInt(1) > 0;
				rs.close();
				if (t.isVersioned()) {
					if (triggerExists) {
						return;
					} else {
						StringBuilder sb = new StringBuilder();
						sb.append(
								String.format("create trigger \"%s\".\"%s_upd\" on \"%s\".\"%s\" for update as begin\n",
										t.getGrain().getName(), t.getName(), t.getGrain().getName(), t.getName()));
						sb.append("IF  exists (select * from inserted inner join deleted on \n");
						addPKJoin(sb, "inserted", "deleted", t);
						sb.append("where inserted.recversion <> deleted.recversion) BEGIN\n");
						sb.append("  RAISERROR ('record version check failure', 16, 1);\n");

						sb.append("END\n");
						sb.append(String.format("update \"%s\".\"%s\" set recversion = recversion + 1 where\n",
								t.getGrain().getName(), t.getName()));
						sb.append("exists (select * from inserted where \n");

						addPKJoin(sb, "inserted", String.format("\"%s\".\"%s\"", t.getGrain().getName(), t.getName()),
								t);

						sb.append(");\nend\n");
						// CREATE TRIGGER
						// System.out.println(sb.toString());

						stmt.executeUpdate(sb.toString());
					}
				} else {
					if (triggerExists) {
						// DROP TRIGGER
						sql = String.format("drop trigger \"%s\".\"%s_upd\"", t.getGrain().getName(), t.getName());
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

	private void addPKJoin(StringBuilder sb, String left, String right, Table t) {
		boolean needAnd = false;
		for (String s : t.getPrimaryKey().keySet()) {
			if (needAnd)
				sb.append(" AND ");
			sb.append(String.format("  %s.\"%s\" = %s.\"%s\"\n", left, s, right, s));
			needAnd = true;
		}
	}

	@Override
	public PreparedStatement getNavigationStatement(Connection conn, GrainElement t, String orderBy,
			String navigationWhereClause) throws CelestaException {
		if (navigationWhereClause == null)
			throw new IllegalArgumentException();

		StringBuilder w = new StringBuilder(navigationWhereClause);
		boolean useWhere = w.length() > 0;
		if (orderBy.length() > 0)
			w.append(" order by " + orderBy);
		String sql = String.format(SELECT_TOP_1 + tableTemplate() + "%s;", getTableFieldsListExceptBLOBs(t),
				t.getGrain().getName(), t.getName(), useWhere ? " where " + w : w);
		// System.out.println(sql);
		return prepareStatement(conn, sql);
	}

	@Override
	public int getDBPid(Connection conn) throws CelestaException {
		try {
			Statement stmt = conn.createStatement();
			try {
				ResultSet rs = stmt.executeQuery("SELECT @@SPID;");
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

	@Override
	public boolean nullsFirst() {
		return true;
	}
}
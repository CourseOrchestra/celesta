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
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.BinaryColumn;
import ru.curs.celesta.score.BooleanColumn;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.DateTimeColumn;
import ru.curs.celesta.score.Expr;
import ru.curs.celesta.score.FloatingColumn;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.GrainElement;
import ru.curs.celesta.score.Index;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.SQLGenerator;
import ru.curs.celesta.score.StringColumn;
import ru.curs.celesta.score.Table;

/**
 * Адаптер Postgres.
 */
final class PostgresAdaptor extends DBAdaptor {

	private static final String SELECT_S_FROM = "select %s from ";
	private static final String NOW = "now()";
	private static final Pattern POSTGRESDATEPATTERN = Pattern
			.compile("(\\d\\d\\d\\d)-(\\d\\d)-(\\d\\d)");
	private static final Pattern HEX_STRING = Pattern
			.compile("'\\\\x([0-9A-Fa-f]+)'");

	private static final Map<Class<? extends Column>, ColumnDefiner> TYPES_DICT = new HashMap<>();
	static {
		TYPES_DICT.put(IntegerColumn.class, new ColumnDefiner() {
			@Override
			String dbFieldType() {
				return "int4";
			}

			@Override
			String getMainDefinition(Column c) {
				return join(c.getQuotedName(), dbFieldType(), nullable(c));
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
		});

		TYPES_DICT.put(FloatingColumn.class, new ColumnDefiner() {

			@Override
			String dbFieldType() {
				return "float8"; // double precision";
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
					defaultStr = DEFAULT + ic.getDefaultValue();
				}
				return defaultStr;
			}
		});

		TYPES_DICT.put(StringColumn.class, new ColumnDefiner() {

			@Override
			String dbFieldType() {
				return "varchar";
			}

			@Override
			String getMainDefinition(Column c) {
				StringColumn ic = (StringColumn) c;
				String fieldType = ic.isMax() ? "text" : String.format(
						"%s(%s)", dbFieldType(), ic.getLength());
				return join(c.getQuotedName(), fieldType, nullable(c));
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

		TYPES_DICT.put(BinaryColumn.class, new ColumnDefiner() {

			@Override
			String dbFieldType() {
				return "bytea";
			}

			@Override
			String getMainDefinition(Column c) {
				return join(c.getQuotedName(), dbFieldType(), nullable(c));
			}

			@Override
			String getDefaultDefinition(Column c) {
				BinaryColumn bc = (BinaryColumn) c;
				String defaultStr = "";
				if (bc.getDefaultValue() != null) {
					Matcher m = HEXSTR.matcher(bc.getDefaultValue());
					m.matches();
					defaultStr = DEFAULT
							+ String.format("E'\\\\x%s'", m.group(1));
				}
				return defaultStr;
			}
		});

		TYPES_DICT.put(DateTimeColumn.class, new ColumnDefiner() {

			@Override
			String dbFieldType() {
				return "timestamp";
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
					defaultStr = DEFAULT + NOW;
				} else if (ic.getDefaultValue() != null) {
					DateFormat df = new SimpleDateFormat("yyyyMMdd");
					defaultStr = String.format(DEFAULT + " '%s'",
							df.format(ic.getDefaultValue()));

				}
				return defaultStr;
			}
		});

		TYPES_DICT.put(BooleanColumn.class, new ColumnDefiner() {

			@Override
			String dbFieldType() {
				return "bool";
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
					defaultStr = DEFAULT + "'" + ic.getDefaultValue() + "'";
				}
				return defaultStr;
			}
		});
	}

	private static final Pattern QUOTED_NAME = Pattern
			.compile("\"?([^\"]+)\"?");

	@Override
	boolean tableExists(Connection conn, String schema, String name)
			throws CelestaException {
		try {
			PreparedStatement check = conn.prepareStatement(String.format(
					"SELECT table_name FROM information_schema.tables  WHERE "
							+ "table_schema = '%s' AND table_name = '%s'",
					schema, name));
			ResultSet rs = check.executeQuery();
			try {
				return rs.next();
			} finally {
				rs.close();
				check.close();
			}
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
	}

	@Override
	boolean userTablesExist(Connection conn) throws SQLException {
		PreparedStatement check = conn
				.prepareStatement("select count(*) from information_schema.tables "
						+ "where table_type = 'BASE TABLE' "
						+ "and table_schema not in ('pg_catalog', 'information_schema');");
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
	void createSchemaIfNotExists(Connection conn, String name)
			throws SQLException {
		// NB: starting from 9.3 version we are able to use
		// 'create schema if not exists' synthax, but we are developing on 9.2
		String sql = String
				.format("SELECT schema_name FROM information_schema.schemata WHERE schema_name = '%s';",
						name);
		Statement check = conn.createStatement();
		ResultSet rs = check.executeQuery(sql);
		try {
			if (!rs.next()) {
				check.executeUpdate(String
						.format("create schema \"%s\";", name));
			}
		} finally {
			rs.close();
			check.close();
		}
	}

	@Override
	ColumnDefiner getColumnDefiner(Column c) {
		return TYPES_DICT.get(c.getClass());
	}

	@Override
	PreparedStatement getOneFieldStatement(Connection conn, Column c)
			throws CelestaException {
		Table t = c.getParentTable();
		String sql = String.format(SELECT_S_FROM + tableTemplate()
				+ " where %s limit 1;", c.getQuotedName(), t.getGrain()
				.getName(), t.getName(), getRecordWhereClause(t));
		return prepareStatement(conn, sql);
	}

	@Override
	PreparedStatement getOneRecordStatement(Connection conn, Table t)
			throws CelestaException {
		String sql = String.format(SELECT_S_FROM + tableTemplate()
				+ " where %s limit 1;", getTableFieldsListExceptBLOBs(t), t
				.getGrain().getName(), t.getName(), getRecordWhereClause(t));
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
				+ " (%s) values (%s);", t.getGrain().getName(), t.getName(),
				fields.toString(), params.toString());

		// System.out.println(sql);

		return prepareStatement(conn, sql);
	}

	@Override
	PreparedStatement getDeleteRecordStatement(Connection conn, Table t)
			throws CelestaException {
		String sql = String.format("delete from " + tableTemplate()
				+ " where %s;", t.getGrain().getName(), t.getName(),
				getRecordWhereClause(t));
		return prepareStatement(conn, sql);
	}

	@Override
	public Set<String> getColumns(Connection conn, Table t)
			throws CelestaException {
		String sql = String.format(
				"select column_name from information_schema.columns "
						+ "where table_schema = '%s' and table_name = '%s';", t
						.getGrain().getName(), t.getName());
		return sqlToStringSet(conn, sql);
	}

	@Override
	PreparedStatement deleteRecordSetStatement(Connection conn, Table t,
			Map<String, AbstractFilter> filters, Expr complexFilter)
			throws CelestaException {
		// Готовим условие where
		String whereClause = getWhereClause(t, filters, complexFilter);

		// Готовим запрос на удаление
		String sql = String.format("delete from " + tableTemplate() + " %s;", t
				.getGrain().getName(), t.getName(),
				whereClause.length() > 0 ? "where " + whereClause : "");
		try {
			PreparedStatement result = conn.prepareStatement(sql);
			// А теперь заполняем параметры
			fillSetQueryParameters(filters, result);
			return result;
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
	}

	@Override
	int getCurrentIdent(Connection conn, Table t) throws CelestaException {
		String sql = String.format("select last_value from \"%s\".\"%s_seq\"",
				t.getGrain().getName(), t.getName());
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
		String fieldList = getFieldList(index.getColumns().keySet());
		String sql = String.format("CREATE INDEX \"%s\" ON " + tableTemplate()
				+ " (%s)", index.getName(), index.getTable().getGrain()
				.getName(), index.getTable().getName(), fieldList);
		return sql;
	}

	@Override
	String getDropIndexSQL(Grain g, DBIndexInfo dBIndexInfo) {
		String sql = String.format("DROP INDEX " + tableTemplate(),
				g.getName(), dBIndexInfo.getIndexName());
		return sql;
	}

	@SuppressWarnings("unchecked")
	@Override
	public DBColumnInfo getColumnInfo(Connection conn, Column c)
			throws CelestaException {
		try {
			DatabaseMetaData metaData = conn.getMetaData();
			ResultSet rs = metaData.getColumns(null, c.getParentTable()
					.getGrain().getName(), c.getParentTable().getName(),
					c.getName());
			try {
				if (rs.next()) {
					DBColumnInfo result = new DBColumnInfo();
					result.setName(rs.getString(COLUMN_NAME));
					String typeName = rs.getString("TYPE_NAME");
					if ("serial".equalsIgnoreCase(typeName)) {
						result.setType(IntegerColumn.class);
						result.setIdentity(true);
						result.setNullable(rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls);
						return result;
					} else if ("text".equalsIgnoreCase(typeName)) {
						result.setType(StringColumn.class);
						result.setMax(true);
					} else {
						for (Class<?> cc : COLUMN_CLASSES)
							if (TYPES_DICT.get(cc).dbFieldType()
									.equalsIgnoreCase(typeName)) {
								result.setType((Class<? extends Column>) cc);
								break;
							}
					}
					result.setNullable(rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls);
					if (result.getType() == StringColumn.class) {
						result.setLength(rs.getInt("COLUMN_SIZE"));
					}
					String defaultBody = rs.getString("COLUMN_DEF");
					if (defaultBody != null) {
						defaultBody = modifyDefault(result, defaultBody);
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

	private String modifyDefault(DBColumnInfo ci, String defaultBody) {
		String result = defaultBody;
		if (DateTimeColumn.class == ci.getType()) {
			if (NOW.equalsIgnoreCase(defaultBody))
				result = "GETDATE()";
			else {
				Matcher m = POSTGRESDATEPATTERN.matcher(defaultBody);
				m.find();
				result = String.format("'%s%s%s'", m.group(1), m.group(2),
						m.group(3));
			}
		} else if (BooleanColumn.class == ci.getType()) {
			result = "'" + defaultBody.toUpperCase() + "'";
		} else if (StringColumn.class == ci.getType()) {
			if (result.endsWith("::text"))
				result = result.substring(0,
						result.length() - "::text".length());
			else if (result.endsWith("::character varying"))
				result = result.substring(0, result.length()
						- "::character varying".length());
		} else if (BinaryColumn.class == ci.getType()) {
			Matcher m = HEX_STRING.matcher(defaultBody);
			if (m.find())
				result = "0x" + m.group(1).toUpperCase();
		}
		return result;
	}

	@Override
	void updateColumn(Connection conn, Column c, DBColumnInfo actual)
			throws CelestaException {
		try {
			String sql;
			List<String> batch = new LinkedList<>();
			// Начинаем с удаления default-значения
			sql = String.format(ALTER_TABLE + tableTemplate()
					+ " ALTER COLUMN \"%s\" DROP DEFAULT", c.getParentTable()
					.getGrain().getName(), c.getParentTable().getName(),
					c.getName());
			batch.add(sql);

			updateColType(c, actual, batch);

			// Проверяем nullability
			if (c.isNullable() != actual.isNullable()) {
				sql = String.format(ALTER_TABLE + tableTemplate()
						+ " ALTER COLUMN \"%s\" %s", c.getParentTable()
						.getGrain().getName(), c.getParentTable().getName(), c
						.getName(), c.isNullable() ? "DROP NOT NULL"
						: "SET NOT NULL");
				batch.add(sql);
			}

			// Если в данных пустой default, а в метаданных -- не пустой -- то
			if (c.getDefaultValue() != null
					|| (c instanceof DateTimeColumn && ((DateTimeColumn) c)
							.isGetdate())) {
				sql = String
						.format(ALTER_TABLE + tableTemplate()
								+ " ALTER COLUMN \"%s\" SET %s", c
								.getParentTable().getGrain().getName(), c
								.getParentTable().getName(), c.getName(),
								getColumnDefiner(c).getDefaultDefinition(c));
				batch.add(sql);
			}

			Statement stmt = conn.createStatement();
			try {
				// System.out.println(">>batch begin>>");
				for (String s : batch) {
					// System.out.println(s);
					stmt.executeUpdate(s);
				}
				// System.out.println("<<batch end<<");
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

	private void updateColType(Column c, DBColumnInfo actual, List<String> batch) {
		String sql;
		String colType;
		if (c.getClass() == StringColumn.class) {
			StringColumn sc = (StringColumn) c;
			colType = sc.isMax() ? "text" : String.format("%s(%s)",
					getColumnDefiner(c).dbFieldType(), sc.getLength());
		} else {
			colType = getColumnDefiner(c).dbFieldType();
		}
		// Если тип не совпадает
		if (c.getClass() != actual.getType()) {
			sql = String.format(ALTER_TABLE + tableTemplate()
					+ " ALTER COLUMN \"%s\" TYPE %s", c.getParentTable()
					.getGrain().getName(), c.getParentTable().getName(),
					c.getName(), colType);
			if (c.getClass() == IntegerColumn.class)
				sql += String
						.format(" USING (%s::integer);", c.getQuotedName());
			else if (c.getClass() == BooleanColumn.class)
				sql += String
						.format(" USING (%s::boolean);", c.getQuotedName());

			batch.add(sql);
		} else if (c.getClass() == StringColumn.class) {
			StringColumn sc = (StringColumn) c;
			if (sc.isMax() != actual.isMax()
					|| sc.getLength() != actual.getLength()) {
				sql = String.format(ALTER_TABLE + tableTemplate()
						+ " ALTER COLUMN \"%s\" TYPE %s", c.getParentTable()
						.getGrain().getName(), c.getParentTable().getName(),
						c.getName(), colType);
				batch.add(sql);
			}
		}
	}

	@Override
	void manageAutoIncrement(Connection conn, Table t) throws SQLException {
		String sql;
		Statement stmt = conn.createStatement();
		try {
			// 1. Firstly, we have to clean up table from any auto-increment
			// defaults. Meanwhile we check if table has IDENTITY field, if it
			// doesn't, no need to proceed.
			IntegerColumn idColumn = null;
			for (Column c : t.getColumns().values())
				if (c instanceof IntegerColumn) {
					IntegerColumn ic = (IntegerColumn) c;
					if (ic.isIdentity())
						idColumn = ic;
					else {
						if (ic.getDefaultValue() == null) {
							sql = String
									.format("alter table %s.%s alter column %s drop default",
											t.getGrain().getQuotedName(),
											t.getQuotedName(),
											ic.getQuotedName());
						} else {
							sql = String
									.format("alter table %s.%s alter column %s set default %d",
											t.getGrain().getQuotedName(), t
													.getQuotedName(), ic
													.getQuotedName(), ic
													.getDefaultValue()
													.intValue());
						}
						stmt.executeUpdate(sql);
					}
				}

			if (idColumn == null)
				return;

			// 2. Now, we know that we surely have IDENTITY field, and we have
			// to be sure that we have an appropriate sequence.
			boolean hasSequence = false;
			sql = String
					.format("select count(*) from pg_class c inner join pg_namespace n ON n.oid = c.relnamespace "
							+ "where n.nspname = '%s' and c.relname = '%s_seq' and c.relkind = 'S'",
							t.getGrain().getName(), t.getName());
			ResultSet rs = stmt.executeQuery(sql);
			rs.next();
			try {
				hasSequence = rs.getInt(1) > 0;
			} finally {
				rs.close();
			}
			if (!hasSequence) {
				sql = String
						.format("create sequence \"%s\".\"%s_seq\" increment 1 minvalue 1",
								t.getGrain().getName(), t.getName());
				stmt.executeUpdate(sql);
			}

			// 3. Now we have to create the auto-increment default
			sql = String.format(
					"alter table %s.%s alter column %s set default "
							+ "nextval('\"%s\".\"%s_seq\"'::regclass);", t
							.getGrain().getQuotedName(), t.getQuotedName(),
					idColumn.getQuotedName(), t.getGrain().getName(), t
							.getName());
			stmt.executeUpdate(sql);
		} finally {
			stmt.close();
		}
	}

	@Override
	void dropAutoIncrement(Connection conn, Table t) throws SQLException {
		// Удаление Sequence
		String sql = String.format("drop sequence if exists \"%s\".\"%s_seq\"",
				t.getGrain().getName(), t.getName());
		Statement stmt = conn.createStatement();
		try {
			stmt.execute(sql);
		} finally {
			stmt.close();
		}
	}

	@Override
	DBPKInfo getPKInfo(Connection conn, Table t) throws CelestaException {
		String sql = String
				.format("SELECT i.relname AS indexname, "
						+ "i.oid, array_length(x.indkey, 1) as colcount "
						+ "FROM pg_index x "
						+ "INNER JOIN pg_class c ON c.oid = x.indrelid "
						+ "INNER JOIN pg_class i ON i.oid = x.indexrelid "
						+ "INNER JOIN pg_namespace n ON n.oid = c.relnamespace "
						+ "WHERE c.relkind = 'r'::\"char\" AND i.relkind = 'i'::\"char\" "
						+ "and n.nspname = '%s' and c.relname = '%s' and x.indisprimary",
						t.getGrain().getName(), t.getName());
		DBPKInfo result = new DBPKInfo();
		try {
			Statement stmt = conn.createStatement();
			PreparedStatement stmt2 = conn
					.prepareStatement("select pg_get_indexdef(?, ?, false)");
			try {
				ResultSet rs = stmt.executeQuery(sql);
				if (rs.next()) {
					String indName = rs.getString("indexname");
					int colCount = rs.getInt("colcount");
					int oid = rs.getInt("oid");
					result.setName(indName);
					stmt2.setInt(1, oid);
					for (int i = 1; i <= colCount; i++) {
						stmt2.setInt(2, i);
						ResultSet rs2 = stmt2.executeQuery();
						try {
							rs2.next();
							String colName = rs2.getString(1);
							Matcher m = QUOTED_NAME.matcher(colName);
							m.matches();
							result.addColumnName(m.group(1));
						} finally {
							rs2.close();
						}
					}
				}
			} finally {
				stmt.close();
				stmt2.close();
			}
		} catch (SQLException e) {
			throw new CelestaException("Could not get indices information: %s",
					e.getMessage());
		}
		return result;

	}

	@Override
	void dropPK(Connection conn, Table t, String pkName)
			throws CelestaException {
		String sql = String.format(
				"alter table %s.%s drop constraint \"%s\" cascade", t
						.getGrain().getQuotedName(), t.getQuotedName(), pkName);
		try {
			Statement stmt = conn.createStatement();
			try {
				stmt.executeUpdate(sql);
			} finally {
				stmt.close();
			}
		} catch (SQLException e) {
			throw new CelestaException("Cannot drop PK '%s': %s", pkName,
					e.getMessage());
		}
	}

	@Override
	void createPK(Connection conn, Table t) throws CelestaException {
		StringBuilder sql = new StringBuilder();
		sql.append(String.format(
				"alter table %s.%s add constraint \"%s\" primary key (", t
						.getGrain().getQuotedName(), t.getQuotedName(), t
						.getPkConstraintName()));
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

		// System.out.println(sql.toString());
		try {
			Statement stmt = conn.createStatement();
			try {
				stmt.executeUpdate(sql.toString());
			} finally {
				stmt.close();
			}
		} catch (SQLException e) {
			throw new CelestaException("Cannot create PK '%s': %s",
					t.getPkConstraintName(), e.getMessage());
		}
	}

	@Override
	List<DBFKInfo> getFKInfo(Connection conn, Grain g) throws CelestaException {
		// Full foreign key information query
		String sql = String
				.format("SELECT RC.CONSTRAINT_SCHEMA AS GRAIN"
						+ "   , KCU1.CONSTRAINT_NAME AS FK_CONSTRAINT_NAME"
						+ "   , KCU1.TABLE_NAME AS FK_TABLE_NAME"
						+ "   , KCU1.COLUMN_NAME AS FK_COLUMN_NAME"
						+ "   , KCU2.TABLE_SCHEMA AS REF_GRAIN"
						+ "   , KCU2.TABLE_NAME AS REF_TABLE_NAME"
						+ "   , RC.UPDATE_RULE, RC.DELETE_RULE "
						+ "FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC "
						+ "INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1 "
						+ "   ON  KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG"
						+ "   AND KCU1.CONSTRAINT_SCHEMA  = RC.CONSTRAINT_SCHEMA"
						+ "   AND KCU1.CONSTRAINT_NAME    = RC.CONSTRAINT_NAME "
						+ "INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU2"
						+ "   ON  KCU2.CONSTRAINT_CATALOG = RC.UNIQUE_CONSTRAINT_CATALOG"
						+ "   AND KCU2.CONSTRAINT_SCHEMA  = RC.UNIQUE_CONSTRAINT_SCHEMA"
						+ "   AND KCU2.CONSTRAINT_NAME    = RC.UNIQUE_CONSTRAINT_NAME"
						+ "   AND KCU2.ORDINAL_POSITION   = KCU1.ORDINAL_POSITION "
						+ "WHERE RC.CONSTRAINT_SCHEMA = '%s' "
						+ "ORDER BY KCU1.CONSTRAINT_NAME, KCU1.ORDINAL_POSITION",
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
	String getLimitedSQL(GrainElement t, String whereClause, String orderBy,
			long offset, long rowCount) {
		if (offset == 0 && rowCount == 0)
			throw new IllegalArgumentException();
		String sql;
		if (offset == 0)
			sql = getSelectFromOrderBy(t, whereClause, orderBy)
					+ String.format(" limit %d", rowCount);
		else if (rowCount == 0)
			sql = getSelectFromOrderBy(t, whereClause, orderBy)
					+ String.format(" limit all offset %d", offset);
		else {
			sql = getSelectFromOrderBy(t, whereClause, orderBy)
					+ String.format(" limit %d offset %d", rowCount, offset);
		}
		return sql;
	}

	@Override
	Map<String, DBIndexInfo> getIndices(Connection conn, Grain g)
			throws CelestaException {
		String sql = String
				.format("SELECT c.relname AS tablename, i.relname AS indexname, "
						+ "i.oid, array_length(x.indkey, 1) as colcount "
						+ "FROM pg_index x "
						+ "INNER JOIN pg_class c ON c.oid = x.indrelid "
						+ "INNER JOIN pg_class i ON i.oid = x.indexrelid "
						+ "INNER JOIN pg_namespace n ON n.oid = c.relnamespace "
						+ "WHERE c.relkind = 'r'::\"char\" AND i.relkind = 'i'::\"char\" "
						+ "and n.nspname = '%s' and x.indisunique = false;",
						g.getName());
		Map<String, DBIndexInfo> result = new HashMap<>();
		try {
			Statement stmt = conn.createStatement();
			PreparedStatement stmt2 = conn
					.prepareStatement("select pg_get_indexdef(?, ?, false)");
			try {
				ResultSet rs = stmt.executeQuery(sql);
				while (rs.next()) {
					String tabName = rs.getString("tablename");
					String indName = rs.getString("indexname");
					DBIndexInfo ii = new DBIndexInfo(tabName, indName);
					result.put(indName, ii);
					int colCount = rs.getInt("colcount");
					int oid = rs.getInt("oid");
					stmt2.setInt(1, oid);
					for (int i = 1; i <= colCount; i++) {
						stmt2.setInt(2, i);
						ResultSet rs2 = stmt2.executeQuery();
						try {
							rs2.next();
							String colName = rs2.getString(1);
							Matcher m = QUOTED_NAME.matcher(colName);
							m.matches();
							ii.getColumnNames().add(m.group(1));
						} finally {
							rs2.close();
						}
					}

				}
			} finally {
				stmt.close();
				stmt2.close();
			}
		} catch (SQLException e) {
			throw new CelestaException("Could not get indices information: %s",
					e.getMessage());
		}
		return result;
	}

	@Override
	public SQLGenerator getViewSQLGenerator() {
		return new SQLGenerator();
	}

	@Override
	public void createSysObjects(Connection conn) throws CelestaException {
		String sql = "CREATE OR REPLACE FUNCTION celesta.recversion_check()"
				+ "  RETURNS trigger AS $BODY$ BEGIN\n"
				+ "    IF (OLD.recversion = NEW.recversion) THEN\n"
				+ "       NEW.recversion = NEW.recversion + 1;\n     ELSE\n"
				+ "       RAISE EXCEPTION 'record version check failure';\n"
				+ "    END IF;" + "    RETURN NEW; END; $BODY$\n"
				+ "  LANGUAGE plpgsql VOLATILE COST 100;";

		try {
			Statement stmt = conn.createStatement();
			try {
				stmt.executeUpdate(sql);
			} finally {
				stmt.close();
			}
		} catch (SQLException e) {
			throw new CelestaException(
					"Could not create or replace celesta.recversion_check() function: %s",
					e.getMessage());
		}
	}

	@Override
	public void updateVersioningTrigger(Connection conn, Table t)
			throws CelestaException {
		// First of all, we are about to check if trigger exists
		String sql = String
				.format("select count(*) from information_schema.triggers where "
						+ "		event_object_schema = '%s' and event_object_table= '%s'"
						+ "		and trigger_name = 'versioncheck'", t.getGrain()
						.getName(), t.getName());
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
						// CREATE TRIGGER
						sql = String
								.format("CREATE TRIGGER \"versioncheck\""
										+ " BEFORE UPDATE ON "
										+ tableTemplate()
										+ " FOR EACH ROW EXECUTE PROCEDURE celesta.recversion_check();",
										t.getGrain().getName(), t.getName());
						stmt.executeUpdate(sql);
					}
				} else {
					if (triggerExists) {
						// DROP TRIGGER
						sql = String.format("DROP TRIGGER \"versioncheck\" ON "
								+ tableTemplate(), t.getGrain().getName(),
								t.getName());
						stmt.executeUpdate(sql);
					} else {
						return;
					}
				}
			} finally {
				stmt.close();
			}
		} catch (SQLException e) {
			throw new CelestaException(
					"Could not update version check trigger on %s.%s: %s", t
							.getGrain().getName(), t.getName(), e.getMessage());
		}

	}

	@Override
	// CHECKSTYLE:OFF 6 params
	PreparedStatement getNavigationStatement(Connection conn, GrainElement t,
			Map<String, AbstractFilter> filters, Expr complexFilter,
			String orderBy, String navigationWhereClause)
			throws CelestaException {
		// CHECKSTYLE:ON
		if (navigationWhereClause == null)
			throw new IllegalArgumentException();
		StringBuilder w = new StringBuilder(getWhereClause(t, filters,
				complexFilter));
		if (w.length() > 0 && navigationWhereClause.length() > 0)
			w.append(" and ");
		w.append(navigationWhereClause);
		boolean useWhere = w.length() > 0;
		if (orderBy.length() > 0)
			w.append(" order by " + orderBy);
		String sql = String.format(SELECT_S_FROM + tableTemplate()
				+ "%s  limit 1;", getTableFieldsListExceptBLOBs(t), t
				.getGrain().getName(), t.getName(), useWhere ? " where " + w
				: w);
		// System.out.println(sql);
		return prepareStatement(conn, sql);
	}

	@Override
	public void resetIdentity(Connection conn, Table t, int i)
			throws SQLException {
		Statement stmt = conn.createStatement();
		try {
			String sql = String.format(
					"alter sequence \"%s\".\"%s_seq\" restart with %d", t
							.getGrain().getName(), t.getName(), i);
			stmt.executeUpdate(sql);
		} finally {
			stmt.close();
		}
	}

}
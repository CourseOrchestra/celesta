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
import java.util.HashMap;
import java.util.HashSet;
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
import ru.curs.celesta.score.FloatingColumn;
import ru.curs.celesta.score.ForeignKey;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.GrainElement;
import ru.curs.celesta.score.Index;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.SQLGenerator;
import ru.curs.celesta.score.StringColumn;
import ru.curs.celesta.score.Table;

/**
 * Адаптер MуSQL.
 */
final class MySQLAdaptor extends DBAdaptor {

	private static final String SELECT_S_FROM = "select %s from ";
	private static final String CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP";

	/**
	 * Default-значение для IDENTITY-колонок.
	 * 
	 * В связи с застарелым багом MySQL (http://bugs.mysql.com/bug.php?id=6295),
	 * наличие before-триггера, выставляющего not-null значения, не спасает от
	 * ошибки при вставке нулевых значений в ненулевые колонки. Ситуация должна
	 * измениться, начиная в MySQL 5.7 -- тогда эту константу и всё, что с ней
	 * связано, надо будет удалить.
	 * 
	 * Пока же "магическое число" в качестве default-значения (вряд ли кому-то
	 * понадобится задавать такое значение в качестве default в бизнес-решении)
	 * сигнализирует о том, что колонка на самом деле default-значения не имеет,
	 * а "магическое" default служит просто для обхода бага.
	 */
	private static final int IDENTITY_DEFAULT_VALUE = 0xF001C0DE;

	private static final Pattern FIELDTYPE = Pattern
			.compile("(\\w+)(\\((\\d+)\\))?");
	private static final Pattern MYSQLDATEPATTERN = Pattern
			.compile("(\\d\\d\\d\\d)-(\\d\\d)-(\\d\\d)");
	private static final int VARCHARMAX = 4000;

	private static final Map<Class<? extends Column>, ColumnDefiner> TYPES_DICT = new HashMap<>();
	static {
		TYPES_DICT.put(IntegerColumn.class, new ColumnDefiner() {
			@Override
			String dbFieldType() {
				return "int";
			}

			@Override
			String getMainDefinition(Column c) {
				IntegerColumn ic = (IntegerColumn) c;
				String defaultStr = "";
				if (ic.isIdentity()) {
					defaultStr = String.format("DEFAULT %d",
							IDENTITY_DEFAULT_VALUE);
				}
				return join(c.getQuotedName(), dbFieldType(), nullable(c),
						defaultStr);
			}

			@Override
			String getDefaultDefinition(Column c) {
				IntegerColumn ic = (IntegerColumn) c;
				String defaultStr = "";
				if (!ic.isIdentity() && ic.getDefaultValue() != null) {
					defaultStr = DEFAULT + ic.getDefaultValue();
				}
				return defaultStr;
			}

		}

		);

		TYPES_DICT.put(FloatingColumn.class, new ColumnDefiner() {

			@Override
			String dbFieldType() {
				return "double";
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

		}

		);
		TYPES_DICT.put(StringColumn.class, new ColumnDefiner() {

			@Override
			String dbFieldType() {
				return "varchar";
			}

			@Override
			String getMainDefinition(Column c) {
				StringColumn ic = (StringColumn) c;
				// See
				// http://stackoverflow.com/questions/332798/equivalent-of-varcharmax-in-mysql
				String fieldType = String.format("%s(%d)", dbFieldType(),
						ic.isMax() ? VARCHARMAX : ic.getLength());

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
				return "blob";
			}

			@Override
			String getMainDefinition(Column c) {
				/*
				 * Поле в MySQL не может быть одновременно not null и default в
				 * силу известного бага (before-триггер срабатывает после
				 * проверки на not null).
				 */
				String nullable = c.getDefaultValue() == null ? nullable(c)
						: "null";
				return join(c.getQuotedName(), dbFieldType(), nullable);
			}

			@Override
			String getDefaultDefinition(Column c) {
				// MySQL doesn't allow defaults on blobs!
				return "";
			}
		});

		TYPES_DICT.put(DateTimeColumn.class, new ColumnDefiner() {

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
					defaultStr = DEFAULT + CURRENT_TIMESTAMP;
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
				if (ic.getDefaultValue() == Boolean.TRUE) {
					defaultStr = DEFAULT + "b'1'";
				} else if (ic.getDefaultValue() == Boolean.FALSE) {
					defaultStr = DEFAULT + "b'0'";
				}
				return defaultStr;
			}
		});
	}

	@Override
	boolean tableExists(Connection conn, String schema, String name)
			throws CelestaException {
		String sql = String.format(
				"select count(*) from information_schema.tables "
						+ "where table_schema = '%s' and table_name = '%s'",
				schema, name);
		try {
			Statement check = conn.createStatement();
			ResultSet rs = check.executeQuery(sql);
			try {
				rs.next();
				return rs.getInt(1) > 0;
			} finally {
				check.close();
			}
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
	}

	@Override
	boolean userTablesExist(Connection conn) throws SQLException {
		String sql = "select count(*) from information_schema.tables "
				+ "where table_schema not in ('information_schema', 'performance_schema', 'mysql')";

		Statement check = conn.createStatement();
		ResultSet rs = check.executeQuery(sql);
		try {
			rs.next();
			return rs.getInt(1) > 0;
		} finally {
			check.close();
		}
	}

	@Override
	void createSchemaIfNotExists(Connection conn, String name)
			throws SQLException {
		String sql = String.format("create schema if not exists %s", name);
		Statement stmt = conn.createStatement();
		try {
			stmt.executeUpdate(sql);
		} finally {
			stmt.close();
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

		String sql = String.format("insert " + tableTemplate()
				+ " (%s) values (%s);", t.getGrain().getName(), t.getName(),
				fields.toString(), params.toString());
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
		String sql = String
				.format("select seqvalue from celesta.sequences where grainid = '%s' and tablename = '%s'",
						t.getGrain().getName(), t.getName());
		try {
			Statement stmt = conn.createStatement();
			try {
				ResultSet rs = stmt.executeQuery(sql);
				if (!rs.next())
					throw new CelestaException(
							"Celesta sequense for %s.%s is not initialized.", t
									.getGrain().getName(), t.getName());
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
		String fieldList = getFieldList(index.getColumns().keySet());
		String sql = String.format("CREATE INDEX \"%s\" ON " + tableTemplate()
				+ " (%s)", index.getName(), index.getTable().getGrain()
				.getName(), index.getTable().getName(), fieldList);
		String[] result = {sql};
		return result;

	}

	@Override
	String[] getDropIndexSQL(Grain g, DBIndexInfo dBIndexInfo) {
		String sql = String.format("DROP INDEX %s ON " + tableTemplate(),
				dBIndexInfo.getIndexName(), g.getName(),
				dBIndexInfo.getTableName());
		String[] result = {sql};
		return result;
	}

	private boolean checkForIncrementTrigger(Connection conn, Column c)
			throws SQLException {
		String sql = String.format("show create trigger %s.\"%s_inc\"", c
				.getParentTable().getGrain().getQuotedName(), c
				.getParentTable().getName());
		Statement checkForTrigger = conn.createStatement();
		try {
			try {
				ResultSet rs = checkForTrigger.executeQuery(sql);
				if (rs.next()) {
					String body = rs.getString("SQL Original Statement");
					if (body != null
							&& body.contains(String.format("/*IDENTITY %s*/",
									c.getName())))
						return true;
				}
			} finally {
				checkForTrigger.close();
			}
		} catch (SQLException e) {
			// trigger does not exist! -- return nothing.
			return false;
		}
		return false;
	}

	private String getBlobDefault(Connection conn, Column c)
			throws SQLException {
		String sql = String.format("show create trigger %s.\"%s_inc\"", c
				.getParentTable().getGrain().getQuotedName(), c
				.getParentTable().getName());

		Pattern search = Pattern
				.compile(String.format(
						"/\\*%s DEFAULT 0x(([0-9A-Fa-f][0-9A-Fa-f])+)\\*/",
						c.getName()));

		Statement checkForTrigger = conn.createStatement();
		try {
			try {
				ResultSet rs = checkForTrigger.executeQuery(sql);
				if (rs.next()) {
					String body = rs.getString("SQL Original Statement");
					if (body == null)
						return "";
					Matcher m = search.matcher(body);
					if (m.find()) {
						String result = "0x" + m.group(1);
						return result;
					}
				}
			} finally {
				checkForTrigger.close();
			}
		} catch (SQLException e) {
			// trigger does not exist! -- return nothing.
			return "";
		}
		return "";
	}

	@SuppressWarnings("unchecked")
	@Override
	public DBColumnInfo getColumnInfo(Connection conn, Column c)
			throws CelestaException {
		try {
			Statement stmt = conn.createStatement();

			ResultSet rs = stmt.executeQuery(String.format(
					"show columns from %s.%s where field = '%s'", c
							.getParentTable().getGrain().getQuotedName(), c
							.getParentTable().getQuotedName(), c.getName()));
			try {
				if (rs.next()) {
					DBColumnInfo result = new DBColumnInfo();
					result.setName(rs.getString("Field"));
					Matcher m = FIELDTYPE.matcher(rs.getString("Type"));
					m.matches();
					String typeName = m.group(1);
					int len = m.group(3) == null ? 0 : Integer.parseInt(m
							.group(3));
					if ("int".equalsIgnoreCase(typeName)) {
						result.setType(IntegerColumn.class);
						result.setIdentity(checkForIncrementTrigger(conn, c));
					} else {
						for (Class<?> cc : COLUMN_CLASSES)
							if (TYPES_DICT.get(cc).dbFieldType()
									.equalsIgnoreCase(typeName)) {
								result.setType((Class<? extends Column>) cc);
								break;
							}
					}
					result.setNullable("yes".equalsIgnoreCase(rs
							.getString("Null")));
					if (result.getType() == StringColumn.class) {
						result.setLength(len);
						result.setMax(len == VARCHARMAX);
					}
					String defaultBody = rs.getString("Default");
					if (rs.wasNull())
						defaultBody = null;
					if (defaultBody != null) {
						defaultBody = modifyDefault(result, defaultBody);

						result.setDefaultValue(defaultBody);
					} else if (result.getType() == BinaryColumn.class) {
						result.setDefaultValue(getBlobDefault(conn, c));
						// Если есть defaultvalue -- успокоим компаратор тем
						// значением, которое он ожидает увидеть.
						result.setNullable(result.getDefaultValue() == null ? result
								.isNullable() : c.isNullable());
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
		// System.out.println(result);

		if (DateTimeColumn.class == ci.getType()) {
			if (CURRENT_TIMESTAMP.equalsIgnoreCase(defaultBody))
				result = "GETDATE()";
			else {
				Matcher m = MYSQLDATEPATTERN.matcher(defaultBody);
				m.find();
				result = String.format("'%s%s%s'", m.group(1), m.group(2),
						m.group(3));
			}
		} else if (IntegerColumn.class == ci.getType()
				&& String.valueOf(IDENTITY_DEFAULT_VALUE).equals(defaultBody))
			result = "";
		else if (BooleanColumn.class == ci.getType()) {
			if ("B'1'".equalsIgnoreCase(defaultBody))
				result = "'TRUE'";
			else if ("B'1'".equalsIgnoreCase(defaultBody))
				result = "'FALSE'";
		} else if (StringColumn.class == ci.getType()) {
			result = StringColumn.quoteString(defaultBody);
		}
		return result;
	}

	@Override
	void updateColumn(Connection conn, Column c, DBColumnInfo actual)
			throws CelestaException {
		String def = columnDef(c);
		String sql = String.format("ALTER TABLE " + tableTemplate()
				+ " MODIFY COLUMN %s", c.getParentTable().getGrain().getName(),
				c.getParentTable().getName(), def);
		// System.out.println(sql);
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

	@Override
	void manageAutoIncrement(Connection conn, Table t) throws SQLException {
		// 1. Firstly, we have to clean up table from any auto-increment
		// triggers
		String triggerName = String.format("\"%s\".\"%s_inc\"", t.getGrain()
				.getName(), t.getName());
		String sql = String.format("drop trigger %s", triggerName);
		Statement stmt = conn.createStatement();
		try {
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			// do nothing
			sql = "";
		} finally {
			stmt.close();
		}

		// 2. Check if table has IDENTITY field and/or BLOBs with defaults, if
		// it doesn't, no need to proceed.
		IntegerColumn ic = null;
		List<BinaryColumn> l = new LinkedList<>();
		for (Column c : t.getColumns().values())
			if (c instanceof IntegerColumn && ((IntegerColumn) c).isIdentity()) {
				ic = (IntegerColumn) c;
			} else if (c instanceof BinaryColumn
					&& ((BinaryColumn) c).getDefaultValue() != null)
				l.add((BinaryColumn) c);
		if (ic == null && l.isEmpty())
			return;

		// 3. Now, we know that we surely have IDENTITY field, and we must
		// assure that we have an appropriate sequence.
		sql = String
				.format("insert into celesta.sequences (grainid, tablename) values ('%s', '%s')",
						t.getGrain().getName(), t.getName());
		// System.out.println(sql);
		stmt = conn.createStatement();
		try {
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			// do nothing
			sql = "";
		} finally {
			stmt.close();
		}

		// 4. Now we have to create the auto-increment trigger
		StringBuilder body = new StringBuilder();
		body.append(String.format(
				"create trigger %s before insert on %s.%s for each row\n",
				triggerName, t.getGrain().getQuotedName(), t.getQuotedName()));
		body.append("begin\n");

		if (ic != null) {
			body.append(String.format("  /*IDENTITY %s*/\n", ic.getName()));
			body.append("  declare  x int;\n");
			body.append(String
					.format("  set x = (SELECT seqvalue FROM celesta.sequences WHERE grainid = '%s' and tablename = '%s') + 1;\n",
							t.getGrain().getName(), t.getName()));
			body.append(String
					.format("  update celesta.sequences set seqvalue = x WHERE grainid = '%s' and tablename = '%s';\n",
							t.getGrain().getName(), t.getName()));
			body.append(String.format("  set new.%s = x;\n", ic.getQuotedName()));
		}
		for (BinaryColumn bc : l) {
			body.append(String.format("  /*%s DEFAULT %s*/\n", bc.getName(),
					bc.getDefaultValue()));
			body.append(String.format("  if new.%s is null then\n",
					bc.getQuotedName()));
			Matcher m = HEXSTR.matcher(bc.getDefaultValue());
			m.matches();
			body.append(String.format("    set new.%s = x'%s';\n",
					bc.getQuotedName(), m.group(1)));
			body.append("  end if;\n");
		}
		body.append("end");

		// System.out.println(body.toString());

		stmt = conn.createStatement();
		try {
			stmt.executeUpdate(body.toString());
		} finally {
			stmt.close();
		}

	}

	@Override
	void dropAutoIncrement(Connection conn, Table t) throws SQLException {
		String sql = String
				.format("delete from celesta.sequences where grainid = '%s' and tablename = '%s';\n",
						t.getGrain().getName(), t.getName());
		Statement stmt = conn.createStatement();
		try {
			stmt.executeUpdate(sql);
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
					.format("select cons.CONSTRAINT_NAME, cols.COLUMN_NAME from INFORMATION_SCHEMA.KEY_COLUMN_USAGE cols "
							+ "inner join INFORMATION_SCHEMA.TABLE_CONSTRAINTS cons "
							+ "on cols.TABLE_SCHEMA = cons.TABLE_SCHEMA "
							+ "and cols.TABLE_NAME = cons.TABLE_NAME "
							+ "and cols.CONSTRAINT_NAME = cons.CONSTRAINT_NAME "
							+ "where cons.CONSTRAINT_TYPE = 'PRIMARY KEY' and cons.TABLE_SCHEMA = '%s' "
							+ "and cons.TABLE_NAME = '%s' order by ORDINAL_POSITION",
							t.getGrain().getName(), t.getName());
			// System.out.println(sql);
			Statement check = conn.createStatement();
			ResultSet rs = check.executeQuery(sql);
			try {
				while (rs.next()) {
					String pkName = rs.getString(1);
					if (!"PRIMARY".equals(pkName))
						throw new CelestaException(
								"Expected PRIMARY for MySQL PK name, found %s",
								pkName);
					// To make comparision happy
					result.setName(t.getPkConstraintName());
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
		String sql = String.format("alter table %s.%s drop primary key", t
				.getGrain().getQuotedName(), t.getQuotedName());
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
		sql.append(String.format("alter table %s.%s add primary key (", t
				.getGrain().getQuotedName(), t.getQuotedName()));
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
				.format("SELECT RC.CONSTRAINT_SCHEMA AS 'GRAIN'"
						+ "   , KCU1.CONSTRAINT_NAME AS 'FK_CONSTRAINT_NAME'"
						+ "   , KCU1.TABLE_NAME AS 'FK_TABLE_NAME'"
						+ "   , KCU1.COLUMN_NAME AS 'FK_COLUMN_NAME'"
						+ "   , KCU2.TABLE_SCHEMA AS 'REF_GRAIN'"
						+ "   , KCU2.TABLE_NAME AS 'REF_TABLE_NAME'"
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
						+ "   AND KCU2.TABLE_NAME =  RC.REFERENCED_TABLE_NAME "
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
			// Маразм? Но это ровно то, что написано в
			// http://dev.mysql.com/doc/refman/5.7/en/select.html
			sql = getSelectFromOrderBy(t, whereClause, orderBy)
					+ String.format(" limit %d,18446744073709551615", offset);
		else {
			sql = getSelectFromOrderBy(t, whereClause, orderBy)
					+ String.format(" limit %d,%d", offset, rowCount);
		}
		return sql;
	}

	@Override
	public boolean isValidConnection(Connection conn, int timeout)
			throws CelestaException {
		boolean result = super.isValidConnection(conn, timeout);
		if (result) {
			try {
				Statement stmt = conn.createStatement();
				try {
					ResultSet rs = stmt
							.executeQuery("SELECT @@GLOBAL.sql_mode");
					rs.next();
					String val = rs.getString(1);
					if (!val.contains("ANSI_QUOTES"))
						throw new CelestaException(
								"sql_mode variable for given MySQL database should contain ANSI_QUOTES, contains only %s.",
								val);
				} finally {
					stmt.close();
				}
			} catch (SQLException e) {
				result = false;
			}
		}
		return result;
	}

	@Override
	public Set<String> getColumns(Connection conn, Table t)
			throws CelestaException {
		String sql = String
				.format("select column_name from information_schema.columns where table_schema = '%s' and table_name = '%s'",
						t.getGrain().getName(), t.getName());
		Set<String> result = new LinkedHashSet<>();
		try {
			Statement check = conn.createStatement();
			ResultSet rs = check.executeQuery(sql);
			try {
				while (rs.next()) {
					result.add(rs.getString(1));
				}
			} finally {
				check.close();
			}
		} catch (SQLException e) {
			throw new CelestaException("Error while submitting '%s': %s.", sql,
					e.getMessage());
		}
		return result;
	}

	@Override
	public void dropFK(Connection conn, String grainName, String tableName,
			String fkName) throws CelestaException {
		String sql = String.format("alter table " + tableTemplate()
				+ " drop foreign key \"%s\"", grainName, tableName, fkName);
		try {
			Statement stmt = conn.createStatement();
			try {
				stmt.executeUpdate(sql);
			} finally {
				stmt.close();
			}
		} catch (SQLException e) {
			throw new CelestaException("Cannot drop foreign key '%s': %s",
					fkName, e.getMessage());
		}
	}

	@Override
	Map<String, DBIndexInfo> getIndices(Connection conn, Grain g)
			throws CelestaException {
		Map<String, DBIndexInfo> result = new HashMap<>();
		try {
			Statement stmt = conn.createStatement();
			try {
				for (Table t : g.getTables().values())
					if (tableExists(conn, g.getName(), t.getName())) {
						Set<String> fkKeyNames = new HashSet<>();
						for (ForeignKey fk : t.getForeignKeys())
							fkKeyNames.add(fk.getConstraintName());
						String sql = String
								.format("SHOW INDEX FROM %s.%s WHERE key_name<>'PRIMARY';",
										g.getQuotedName(), t.getQuotedName());
						ResultSet rs = stmt.executeQuery(sql);
						DBIndexInfo i = null;
						while (rs.next()) {
							String indName = rs.getString("Key_name");
							if (fkKeyNames.contains(indName))
								continue;
							if (i == null || !i.getIndexName().equals(indName)) {
								i = new DBIndexInfo(t.getName(), indName);
								result.put(indName, i);
							}
							i.getColumnNames().add(rs.getString("Column_name"));
						}
						rs.close();
					}
			} finally {
				stmt.close();
			}
		} catch (SQLException e) {
			throw new CelestaException("Could not get indices information: %s",
					e.getMessage());
		}
		return result;
	}

	@Override
	public SQLGenerator getViewSQLGenerator() {
		return new SQLGenerator() {
			@Override
			protected void concat(StringBuilder result, List<String> operands) {
				result.append("concat(");
				boolean needOp = false;
				for (String operand : operands) {
					if (needOp)
						result.append(",");
					result.append(operand);
					needOp = true;
				}
				result.append(")");
			}
		};
	}

	@Override
	public void updateVersioningTrigger(Connection conn, Table t)
			throws CelestaException {
		// First of all, we are about to check if trigger exists
		String sql = String.format("show create trigger %s.\"%s_upd\"", t
				.getGrain().getQuotedName(), t.getName());
		try {
			Statement stmt = conn.createStatement();
			try {
				boolean triggerExists = false;
				try {
					ResultSet rs = stmt.executeQuery(sql);
					triggerExists = rs.next();
					rs.close();
				} catch (SQLException e) {
					triggerExists = false;
				}

				if (t.isVersioned()) {
					if (triggerExists) {
						return;
					} else {
						// CREATE TRIGGER
						sql = String
								.format("CREATE trigger \"%s\".\"%s_upd\" before update "
										+ "on \"%s\".\"%s\" for each row\nbegin\n"
										+ "  /*version check*/\n "
										+ "  if old.\"recversion\" <> new.\"recversion\" THEN  \n"
										+ "     SIGNAL SQLSTATE '02000' SET MESSAGE_TEXT = 'record version check failure';\n"
										+ "  end if;\n  set new.\"recversion\" = new.\"recversion\" + 1;\nend",
										t.getGrain().getName(), t.getName(), t
												.getGrain().getName(), t
												.getName());
						stmt.executeUpdate(sql);
					}
				} else {
					if (triggerExists) {
						// DROP TRIGGER
						sql = String.format("DROP TRIGGER \"%s\".\"%s_upd\"", t
								.getGrain().getName(), t.getName());
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
	public int getDBPid(Connection conn) throws CelestaException {
		try {
			Statement stmt = conn.createStatement();
			try {
				ResultSet rs = stmt.executeQuery("select connection_id();");
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
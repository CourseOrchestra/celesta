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
 * Адаптер MSSQL.
 * 
 */
final class MSSQLAdaptor extends DBAdaptor {

	private static final String WHERE_S = " where %s;";

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
				return "real";
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
				String fieldType = String.format("%s(%s)", dbFieldType(),
						ic.isMax() ? "max" : ic.getLength());
				return join(c.getQuotedName(), fieldType, nullable(c));
			}

			@Override
			String getDefaultDefinition(Column c) {
				StringColumn ic = (StringColumn) c;
				String defaultStr = "";
				if (ic.getDefaultValue() != null) {
					defaultStr = msSQLDefault(c)
							+ StringColumn.quoteString(ic.getDefaultValue());
				}
				return defaultStr;
			}

			@Override
			String getLightDefaultDefinition(Column c) {
				StringColumn ic = (StringColumn) c;
				String defaultStr = "";
				if (ic.getDefaultValue() != null) {
					defaultStr = DEFAULT
							+ StringColumn.quoteString(ic.getDefaultValue());
				}
				return defaultStr;
			}
		});

		TYPES_DICT.put(BinaryColumn.class, new MSColumnDefiner() {

			@Override
			String dbFieldType() {
				return "image";
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
					defaultStr = String.format(msSQLDefault(c) + " '%s'",
							df.format(ic.getDefaultValue()));
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
					defaultStr = String.format(DEFAULT + " '%s'",
							df.format(ic.getDefaultValue()));
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
					defaultStr = msSQLDefault(c) + "'" + ic.getDefaultValue()
							+ "'";
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
		return String.format("constraint \"def_%s_%s\" ", c.getParentTable()
				.getName(), c.getName())
				+ ColumnDefiner.DEFAULT;
	}

	@Override
	boolean tableExists(Connection conn, String schema, String name)
			throws CelestaException {
		try {
			PreparedStatement check = conn.prepareStatement(String.format(
					"select coalesce(object_id('%s.%s'), -1)", schema, name));
			ResultSet rs = check.executeQuery();
			try {
				rs.next();
				return rs.getInt(1) != -1;
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
				.prepareStatement("select count(*) from sys.tables;");
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
		PreparedStatement check = conn.prepareStatement(String.format(
				"select coalesce(SCHEMA_ID('%s'), -1)", name));
		ResultSet rs = check.executeQuery();
		try {
			rs.next();
			if (rs.getInt(1) == -1) {
				PreparedStatement create = conn.prepareStatement(String.format(
						"create schema \"%s\";", name));
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
	PreparedStatement getOneFieldStatement(Connection conn, Column c)
			throws CelestaException {
		Table t = c.getParentTable();
		String sql = String.format("select top 1 %s from " + tableTemplate()
				+ WHERE_S, c.getQuotedName(), t.getGrain().getName(),
				t.getName(), getRecordWhereClause(t));
		return prepareStatement(conn, sql);
	}

	@Override
	PreparedStatement getOneRecordStatement(Connection conn, Table t)
			throws CelestaException {
		String sql = String.format("select top 1 %s from " + tableTemplate()
				+ WHERE_S, getTableFieldsListExceptBLOBs(t), t.getGrain()
				.getName(), t.getName(), getRecordWhereClause(t));
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
			fields.append(c);
		}

		String sql = String.format("insert " + tableTemplate()
				+ " (%s) values (%s);", t.getGrain().getName(), t.getName(),
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
				padComma(setClause);
				setClause.append(String.format("%s = ?", c));
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
		String sql = String.format("delete " + tableTemplate() + WHERE_S, t
				.getGrain().getName(), t.getName(), getRecordWhereClause(t));
		return prepareStatement(conn, sql);
	}

	// @Override
	// public Set<String> getColumns(Connection conn, Table t)
	// throws CelestaException {
	// String sql = String
	// .format("select name from sys.columns where object_id = OBJECT_ID('%s.%s');",
	// t.getGrain().getName(), t.getName());
	// return sqlToStringSet(conn, sql);
	// }

	@Override
	PreparedStatement deleteRecordSetStatement(Connection conn, Table t,
			Map<String, AbstractFilter> filters) throws CelestaException {
		// Готовим условие where
		String whereClause = getWhereClause(t, filters);

		// Готовим запрос на удаление
		String sql = String.format("delete " + tableTemplate() + " %s;", t
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
		String sql = String.format("CREATE INDEX %s ON " + tableTemplate()
				+ " (%s)", index.getQuotedName(), index.getTable().getGrain()
				.getName(), index.getTable().getName(), fieldList);
		return sql;
	}

	@Override
	String getDropIndexSQL(Grain g, DBIndexInfo dBIndexInfo) {
		String sql = String.format("DROP INDEX %s ON " + tableTemplate(),
				dBIndexInfo.getIndexName(), g.getName(),
				dBIndexInfo.getTableName());
		return sql;
	}

	private boolean checkIfVarcharMax(Connection conn, Column c)
			throws SQLException {
		PreparedStatement checkForMax = conn.prepareStatement(String.format(
				"select max_length from sys.columns where "
						+ "object_id  = OBJECT_ID('%s.%s') and name = '%s'", c
						.getParentTable().getGrain().getName(), c
						.getParentTable().getName(), c.getName()));
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

	private boolean checkForIncrementTrigger(Connection conn, Column c)
			throws SQLException {
		PreparedStatement checkForTrigger = conn
				.prepareStatement(String
						.format("select text from sys.syscomments where id = object_id('%s.\"%s_inc\"')",
								c.getParentTable().getGrain().getQuotedName(),
								c.getParentTable().getName()));
		try {
			ResultSet rs = checkForTrigger.executeQuery();
			if (rs.next()) {
				String body = rs.getString(1);
				if (body != null
						&& body.contains(String.format("/*IDENTITY %s*/",
								c.getName())))
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
								&& defaultBody.charAt(defaultBody.length() - i
										- 1) == ')') {
							i++;
						}
						defaultBody = defaultBody.substring(i,
								defaultBody.length() - i);
						if (BooleanColumn.class == result.getType()
								|| DateTimeColumn.class == result.getType())
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
	void updateColumn(Connection conn, Column c, DBColumnInfo actual)
			throws CelestaException {

		String sql;
		if (!"".equals(actual.getDefaultValue())) {
			sql = String.format(ALTER_TABLE + tableTemplate()
					+ " drop constraint \"def_%s_%s\"", c.getParentTable()
					.getGrain().getName(), c.getParentTable().getName(), c
					.getParentTable().getName(), c.getName());
			runUpdateColumnSQL(conn, c, sql);
		}

		String def = getColumnDefiner(c).getMainDefinition(c);
		sql = String.format(ALTER_TABLE + tableTemplate() + " alter column %s",
				c.getParentTable().getGrain().getName(), c.getParentTable()
						.getName(), def);
		runUpdateColumnSQL(conn, c, sql);

		def = getColumnDefiner(c).getDefaultDefinition(c);
		if (!"".equals(def)) {
			sql = String.format(ALTER_TABLE + tableTemplate()
					+ " add %s for %s",
					c.getParentTable().getGrain().getName(), c.getParentTable()
							.getName(), def, c.getQuotedName());
			runUpdateColumnSQL(conn, c, sql);
		}

		if (c instanceof IntegerColumn)
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

	@Override
	void manageAutoIncrement(Connection conn, Table t) throws SQLException {
		// 1. Firstly, we have to clean up table from any auto-increment
		// triggers
		String triggerName = String.format("\"%s\".\"%s_inc\"", t.getGrain()
				.getName(), t.getName());
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
		IntegerColumn ic = null;
		for (Column c : t.getColumns().values())
			if (c instanceof IntegerColumn && ((IntegerColumn) c).isIdentity()) {
				ic = (IntegerColumn) c;
				break;
			}
		if (ic == null)
			return;

		// 3. Now, we know that we surely have IDENTITY field, and we must
		// assure that we have an appropriate sequence.
		sql = String
				.format("insert into celesta.sequences (grainid, tablename) values ('%s', '%s')",
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
		body.append(String.format(
				"create trigger %s on %s.%s instead of insert as begin\n",
				triggerName, t.getGrain().getQuotedName(), t.getQuotedName()));
		body.append(String.format("  /*IDENTITY %s*/\n", ic.getName()));
		body.append("  set nocount on;\n");
		body.append("  begin transaction;\n");
		body.append("  declare @id int;\n");
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
				body.append(ColumnDefiner.join(d.getMainDefinition(c),
						d.getLightDefaultDefinition(c)));
				padComma(selectList);
				padComma(insertList);
				selectList.append(c.getQuotedName());
				insertList.append(c.getQuotedName());
			}
			body.append(i.hasNext() ? ",\n" : "\n");
		}
		body.append("  );\n");
		body.append(String.format(
				"  insert into @tmp (%s) select %s from inserted;\n",
				selectList, selectList));
		body.append(String
				.format("  select @id = seqvalue from celesta.sequences where grainid = '%s' and tablename = '%s';\n",
						t.getGrain().getName(), t.getName()));
		body.append(String
				.format("  update celesta.sequences set seqvalue = @id + @@IDENTITY where grainid = '%s' and tablename = '%s';\n",
						t.getGrain().getName(), t.getName()));
		body.append(String.format(
				"  insert into %s.%s (%s) select %s from @tmp;\n", t.getGrain()
						.getQuotedName(), t.getQuotedName(), fullList,
				insertList));
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

	private static void padComma(StringBuilder insertList) {
		if (insertList.length() > 0)
			insertList.append(", ");
	}

	@Override
	void dropAutoIncrement(Connection conn, Table t) throws SQLException {
		String sql = String
				.format("delete from celesta.sequences where grainid = '%s' and tablename = '%s';\n",
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
	DBPKInfo getPrimaryKeyInfo(Connection conn, Table t)
			throws CelestaException {

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
			//System.out.println(sql);
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
	void dropTablePK(Connection conn, Table t, String pkName)
			throws CelestaException {
		String sql = String.format("alter table %s.%s drop constraint \"%s\"",
				t.getGrain().getQuotedName(), t.getQuotedName(), pkName);
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
	void createTablePK(Connection conn, Table t) throws CelestaException {
		StringBuilder sql = new StringBuilder();
		sql.append(String.format("alter table %s.%s add constraint \"%s\" "
				+ " primary key (", t.getGrain().getQuotedName(),
				t.getQuotedName(), t.getPkConstraintName()));
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
						i.setUpdateBehaviour(getFKRule(rs
								.getString("UPDATE_RULE")));
						i.setDeleteBehaviour(getFKRule(rs
								.getString("DELETE_RULE")));
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
}
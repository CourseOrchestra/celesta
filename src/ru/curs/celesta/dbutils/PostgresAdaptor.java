package ru.curs.celesta.dbutils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
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
import ru.curs.celesta.score.FloatingColumn;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.Index;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.StringColumn;
import ru.curs.celesta.score.Table;

/**
 * Адаптер Postgres.
 */
final class PostgresAdaptor extends DBAdaptor {

	private static final Map<Class<? extends Column>, ColumnDefiner> TYPES_DICT = new HashMap<>();
	static {
		TYPES_DICT.put(IntegerColumn.class, new ColumnDefiner() {
			@Override
			String dbFieldType() {
				return "integer";
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
				return "double precision";
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
				String fieldType = String.format("%s(%s)", dbFieldType(),
						ic.isMax() ? "65535" : ic.getLength());
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
					defaultStr = DEFAULT + String.format("E'%s'", m.group(1));
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
					defaultStr = DEFAULT + "CURRENT_TIMESTAMP";
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
		String sql = String.format("select %s from " + tableTemplate()
				+ " where %s limit 1;", c.getQuotedName(), t.getGrain()
				.getName(), t.getName(), getRecordWhereClause(t));
		return prepareStatement(conn, sql);
	}

	@Override
	PreparedStatement getOneRecordStatement(Connection conn, Table t)
			throws CelestaException {
		String sql = String.format("select %s from " + tableTemplate()
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
	PreparedStatement getUpdateRecordStatement(Connection conn, Table t,
			boolean[] equalsMask) throws CelestaException {
		StringBuilder setClause = new StringBuilder();
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
			Map<String, AbstractFilter> filters) throws CelestaException {
		// Готовим условие where
		String whereClause = getWhereClause(t, filters);

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

	@Override
	public DBColumnInfo getColumnInfo(Connection conn, Column c)
			throws CelestaException {
		DBColumnInfo result = new DBColumnInfo();
		// TODO
		return result;
	}

	@Override
	void updateColumn(Connection conn, Column c, DBColumnInfo actual)
			throws CelestaException {
		String def = columnDef(c);
		String sql = String.format("ALTER TABLE " + tableTemplate()
				+ " ALTER COLUMN %s", c.getParentTable().getGrain().getName(),
				c.getParentTable().getName(), def);
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	void dropPK(Connection conn, Table t, String pkName)
			throws CelestaException {
		// TODO Auto-generated method stub

	}

	@Override
	void createPK(Connection conn, Table t) throws CelestaException {
		// TODO Auto-generated method stub

	}

	@Override
	List<DBFKInfo> getFKInfo(Connection conn, Grain g) throws CelestaException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	String getLimitedSQL(Table t, String whereClause, String orderBy,
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
					+ String.format(" limit %d offset %d", offset, rowCount);
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
}
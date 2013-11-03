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

	private static final Map<Class<? extends Column>, ColumnDefiner> TYPES_DICT = new HashMap<>();
	static {
		TYPES_DICT.put(IntegerColumn.class, new ColumnDefiner() {
			@Override
			String dbFieldType() {
				return "number";
			}

			@Override
			String getColumnDef(Column c) {
				IntegerColumn ic = (IntegerColumn) c;
				String defaultStr = "";
				if (ic.getDefaultValue() != null) {
					defaultStr = DEFAULT + ic.getDefaultValue();
				}
				return join(c.getQuotedName(), dbFieldType(), defaultStr,
						nullable(c));
			}

		}

		);

		TYPES_DICT.put(FloatingColumn.class, new ColumnDefiner() {

			@Override
			String dbFieldType() {
				return "real";
			}

			@Override
			String getColumnDef(Column c) {
				FloatingColumn ic = (FloatingColumn) c;
				String defaultStr = "";
				if (ic.getDefaultvalue() != null) {
					defaultStr = DEFAULT + ic.getDefaultvalue();
				}
				return join(c.getQuotedName(), dbFieldType(), defaultStr,
						nullable(c));
			}

		}

		);
		TYPES_DICT.put(StringColumn.class, new ColumnDefiner() {

			@Override
			String dbFieldType() {
				return "varchar2";
			}

			@Override
			String getColumnDef(Column c) {
				StringColumn ic = (StringColumn) c;
				// See
				// http://stackoverflow.com/questions/414817/what-is-the-equivalent-of-varcharmax-in-oracle
				String fieldType = String.format("%s(%s)", dbFieldType(),
						ic.isMax() ? "4000" : ic.getLength());
				String defaultStr = "";
				if (ic.getDefaultValue() != null) {
					defaultStr = DEFAULT
							+ StringColumn.quoteString(ic.getDefaultValue());
				}
				String nullable = (DEFAULT + "''").equals(defaultStr) ? ""
						: nullable(c);
				return join(c.getQuotedName(), fieldType, defaultStr, nullable);
			}

		});
		TYPES_DICT.put(BinaryColumn.class, new ColumnDefiner() {

			@Override
			String dbFieldType() {
				return "blob";
			}

			@Override
			String getColumnDef(Column c) {
				BinaryColumn ic = (BinaryColumn) c;
				String defaultStr = "";
				if (ic.getDefaultValue() != null) {
					// Отрезаем 0x и закавычиваем
					defaultStr = String.format(DEFAULT + "'%s'", ic
							.getDefaultValue().substring(2));
				}
				return join(c.getQuotedName(), dbFieldType(), defaultStr,
						nullable(c));
			}
		});

		TYPES_DICT.put(DateTimeColumn.class, new ColumnDefiner() {

			@Override
			String dbFieldType() {
				return "timestamp";
			}

			@Override
			String getColumnDef(Column c) {
				DateTimeColumn ic = (DateTimeColumn) c;
				String defaultStr = "";
				if (ic.isGetdate()) {
					defaultStr = DEFAULT + "sysdate";
				} else if (ic.getDefaultValue() != null) {
					DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
					defaultStr = String.format(DEFAULT + "date '%s'",
							df.format(ic.getDefaultValue()));
				}
				return join(c.getQuotedName(), dbFieldType(), defaultStr,
						nullable(c));
			}
		});
		TYPES_DICT.put(BooleanColumn.class, new ColumnDefiner() {

			@Override
			String dbFieldType() {
				return "int";
			}

			@Override
			String getColumnDef(Column c) {
				BooleanColumn ic = (BooleanColumn) c;
				String defaultStr = "";
				if (ic.getDefaultValue() != null) {
					defaultStr = DEFAULT + (ic.getDefaultValue() ? "1" : "0");
				}
				String check = String.format("check (%s in (0, 1))",
						c.getQuotedName());
				return join(c.getQuotedName(), dbFieldType(), defaultStr,
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
		DatabaseMetaData metaData = conn.getMetaData();
		String tableName = String.format(tableTemplate(), schema, name)
				.toUpperCase();
		ResultSet rs = metaData.getTables(null, null, tableName,
				new String[] { "TABLE" });
		try {
			if (rs.next()) {
				// String tableSchem = rs.getString("TABLE_SCHEM");
				String rTableName = rs.getString("TABLE_NAME");
				// return schema.equals(tableSchem) && tableName.equals(name);
				return tableName.equals(rTableName);
			}
			return false;
		} finally {
			rs.close();
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
	ColumnDefiner getColumnDefiner(Column c) {
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
	public Map<IndexInfo, TreeMap<Short, String>> getIndices(Connection conn,
			Grain g) throws CelestaException {
		Map<IndexInfo, TreeMap<Short, String>> result = new HashMap<>();
		try {
			for (Table t : g.getTables().values()) {
				String tableName = String.format(tableTemplate(), g.getName(),
						t.getName()).toUpperCase();
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
							indName = indName.toLowerCase(); // TODO: quote
																// index name!!
							String grainPrefix = g.getName().toLowerCase()
									+ "_";
							if (indName.startsWith(grainPrefix))
								indName = indName.substring(grainPrefix
										.length());
							IndexInfo info = new IndexInfo(t.getName(), indName);
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
			String tableName = String.format(tableTemplate(),
					t.getGrain().getName(), t.getName()).toUpperCase();
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
		String sequenceName = String.format(tableTemplate() + "_%s", table
				.getGrain().getName(), table.getName(), col.getName());
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
		return "%s_%s";
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
	String getDropIndexSQL(Grain g, IndexInfo indexInfo) {
		String sql = String.format("DROP INDEX " + tableTemplate(),
				g.getName(), indexInfo.getIndexName());
		return sql;
	}

	@Override
	ColumnInfo getColumnInfo(Column c) {
		// TODO Auto-generated method stub
		return null;
	}
}

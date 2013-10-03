package ru.curs.celesta.dbutils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.BinaryColumn;
import ru.curs.celesta.score.BooleanColumn;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.DateTimeColumn;
import ru.curs.celesta.score.FloatingColumn;
import ru.curs.celesta.score.Grain;
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
				return join(c.getName(), dbFieldType(), defaultStr, nullable(c));
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
				return join(c.getName(), dbFieldType(), defaultStr, nullable(c));
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
				return join(c.getName(), fieldType, defaultStr, nullable);
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
					defaultStr = DEFAULT + ic.getDefaultValue();
				}
				return join(c.getName(), dbFieldType(), defaultStr, nullable(c));
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
					defaultStr = DEFAULT + ic.getDefaultValue();
				}
				return join(c.getName(), dbFieldType(), defaultStr, nullable(c));
			}
		});
		TYPES_DICT.put(BooleanColumn.class, new ColumnDefiner() {

			@Override
			String dbFieldType() {
				return "char(1)";
			}

			@Override
			String getColumnDef(Column c) {
				BooleanColumn ic = (BooleanColumn) c;
				String defaultStr = "";
				if (ic.getDefaultValue() != null) {
					defaultStr = DEFAULT + "'"
							+ (ic.getDefaultValue() ? 'Y' : 'N') + "'";
				}
				return join(c.getName(), dbFieldType(), defaultStr, nullable(c));
			}
		});
	}

	private final String notImplementMsg = "not implemented yet";

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
	PreparedStatement getOneRecordStatement(Connection conn, Table t)
			throws CelestaException {
		String sql = String.format("select %s from " + tableTemplate()
				+ " where %s and rownum = 1", getTableFieldsList(t), t
				.getGrain().getName(), t.getName(), getRecordWhereClause(t));
		return prepareStatement(conn, sql);
	}

	private String getWhereClause(Map<String, AbstractFilter> filters) {
		if (filters == null)
			throw new IllegalArgumentException();
		StringBuilder whereClause = new StringBuilder();
		for (Entry<String, AbstractFilter> e : filters.entrySet()) {
			if (whereClause.length() > 0)
				whereClause.append(" and ");
			if (e.getValue() instanceof SingleValue)
				whereClause.append(String.format("(%s = ?)", e.getKey()));
			else if (e.getValue() instanceof Range)
				whereClause.append(String.format("(%s between ? and ?)",
						e.getKey()));
			else if (e.getValue() instanceof Filter)
				throw new RuntimeException(notImplementMsg);
		}
		return whereClause.toString();
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
			// А теперь заполняем параметры
			int i = 1;
			for (AbstractFilter f : filters.values()) {
				if (f instanceof SingleValue) {
					setParam(result, i, ((SingleValue) f).getValue());
					i++;
				} else if (f instanceof Range) {
					setParam(result, i, ((Range) f).getValueFrom());
					i++;
					setParam(result, i, ((Range) f).getValueTo());
					i++;
				} else if (f instanceof Filter)
					throw new RuntimeException(notImplementMsg);
			}
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
			fields.append(c);
		}

		String sql = String.format("insert into " + tableTemplate()
				+ " (%s) values (%s)", t.getGrain().getName(), t.getName(),
				fields.toString(), params.toString());
		return prepareStatement(conn, sql);
	}

	@Override
	PreparedStatement getUpdateRecordStatement(Connection conn, Table t)
			throws CelestaException {
		StringBuilder setClause = new StringBuilder();
		for (String c : t.getColumns().keySet())
			// Пропускаем ключевые поля
			if (!t.getPrimaryKey().containsKey(c)) {
				if (setClause.length() > 0)
					setClause.append(", ");
				setClause.append(String.format("%s = ?", c));
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
	public Set<String> getIndices(Connection conn, Grain g)
			throws CelestaException {
		Set<String> result = new HashSet<String>();
		try {
			for (Table t : g.getTables().values()) {
				String tableName = String.format(tableTemplate(), g.getName(),
						t.getName()).toUpperCase();
				DatabaseMetaData metaData = conn.getMetaData();
				ResultSet rs = metaData.getIndexInfo(null, null, tableName,
						false, false);
				try {
					while (rs.next()) {
						String rIndexName = rs.getString("INDEX_NAME");
						if (rIndexName != null) {
							result.add(rIndexName.toLowerCase());
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
		Set<String> result = new HashSet<String>();
		try {
			String tableName = String.format(tableTemplate(),
					t.getGrain().getName(), t.getName()).toUpperCase();
			DatabaseMetaData metaData = conn.getMetaData();
			ResultSet rs = metaData.getColumns(null, null, tableName, null);
			try {
				while (rs.next()) {
					String rColumnName = rs.getString("COLUMN_NAME");
					result.add(rColumnName.toLowerCase());
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
				int i = 1;
				for (AbstractFilter f : filters.values()) {
					if (f instanceof SingleValue) {
						setParam(result, i, ((SingleValue) f).getValue());
						i++;
					} else if (f instanceof Range) {
						setParam(result, i, ((Range) f).getValueFrom());
						i++;
						setParam(result, i, ((Range) f).getValueTo());
						i++;
					} else if (f instanceof Filter)
						throw new RuntimeException(notImplementMsg);
				}
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
		// Создание Sequence
		String sequenceName = String.format(tableTemplate() + "_%s", table
				.getGrain().getName(), table.getName(), col.getName());
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
				.getGrain().getName(), table.getName(), col.getName(), col
				.getName());
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
		String sequenceName = String.format(tableTemplate() + "_%s", table
				.getGrain().getName(), table.getName(), col.getName());
		String sql = "DROP SEQUENCE " + sequenceName;
		Statement stmt = conn.createStatement();
		try {
			stmt.execute(sql);
		} finally {
			stmt.close();
		}
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
				sequenceName = String.format(tableTemplate() + "_%s", t
						.getGrain().getName(), t.getName(), col.getName());
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
}

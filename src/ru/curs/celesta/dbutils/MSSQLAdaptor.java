package ru.curs.celesta.dbutils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
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
	private static final Map<Class<? extends Column>, ColumnDefiner> TYPES_DICT = new HashMap<>();
	static {
		TYPES_DICT.put(IntegerColumn.class, new ColumnDefiner() {
			@Override
			String dbFieldType() {
				return "int";
			}

			@Override
			String getColumnDef(Column c) {
				IntegerColumn ic = (IntegerColumn) c;
				String defaultStr = "";
				if (ic.isIdentity()) {
					defaultStr = "IDENTITY";
				} else if (ic.getDefaultValue() != null) {
					defaultStr = DEFAULT + ic.getDefaultValue();
				}
				return join(c.getQuotedName(), dbFieldType(), nullable(c),
						defaultStr);
			}
		});

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
				return join(c.getQuotedName(), dbFieldType(), nullable(c),
						defaultStr);
			}
		});

		TYPES_DICT.put(StringColumn.class, new ColumnDefiner() {

			@Override
			String dbFieldType() {
				return "nvarchar";
			}

			@Override
			String getColumnDef(Column c) {
				StringColumn ic = (StringColumn) c;
				String fieldType = String.format("%s(%s)", dbFieldType(),
						ic.isMax() ? "max" : ic.getLength());
				String defaultStr = "";
				if (ic.getDefaultValue() != null) {
					defaultStr = DEFAULT
							+ StringColumn.quoteString(ic.getDefaultValue());
				}
				return join(c.getQuotedName(), fieldType, nullable(c),
						defaultStr);
			}
		});

		TYPES_DICT.put(BinaryColumn.class, new ColumnDefiner() {

			@Override
			String dbFieldType() {
				return "image";
			}

			@Override
			String getColumnDef(Column c) {
				BinaryColumn ic = (BinaryColumn) c;
				String defaultStr = "";
				if (ic.getDefaultValue() != null) {
					defaultStr = DEFAULT + ic.getDefaultValue();
				}
				return join(c.getQuotedName(), dbFieldType(), nullable(c),
						defaultStr);
			}
		});

		TYPES_DICT.put(DateTimeColumn.class, new ColumnDefiner() {

			@Override
			String dbFieldType() {
				return "datetime";
			}

			@Override
			String getColumnDef(Column c) {
				DateTimeColumn ic = (DateTimeColumn) c;
				String defaultStr = "";
				if (ic.isGetdate()) {
					defaultStr = DEFAULT + "getdate()";
				} else if (ic.getDefaultValue() != null) {
					DateFormat df = new SimpleDateFormat("yyyyMMdd");
					defaultStr = String.format(DEFAULT + " '%s'",
							df.format(ic.getDefaultValue()));
				}
				return join(c.getQuotedName(), dbFieldType(), nullable(c),
						defaultStr);
			}
		});

		TYPES_DICT.put(BooleanColumn.class, new ColumnDefiner() {

			@Override
			String dbFieldType() {
				return "bit";
			}

			@Override
			String getColumnDef(Column c) {
				BooleanColumn ic = (BooleanColumn) c;
				String defaultStr = "";
				if (ic.getDefaultValue() != null) {
					defaultStr = DEFAULT + "'" + ic.getDefaultValue() + "'";
				}
				return join(c.getQuotedName(), dbFieldType(), nullable(c),
						defaultStr);
			}
		});
	}

	@Override
	boolean tableExists(Connection conn, String schema, String name)
			throws SQLException {
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
	ColumnDefiner getColumnDefiner(Column c) {
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
	PreparedStatement getRecordSetStatement(Connection conn, Table t,
			Map<String, AbstractFilter> filters, List<String> orderBy)
			throws CelestaException {

		// Готовим условие where
		String whereClause = getWhereClause(filters);

		// Соединяем полученные компоненты в стандартный запрос
		// SELECT..FROM..WHERE..ORDER BY
		String sql = getSelectFromOrderBy(t, whereClause, orderBy);

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
				if (setClause.length() > 0)
					setClause.append(", ");
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
		String whereClause = getWhereClause(filters);

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
		PreparedStatement stmt = prepareStatement(conn, String.format(
				"SELECT IDENT_CURRENT('%s.%s')", t.getGrain().getName(),
				t.getName()));
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
		String fieldList = getFieldList(index.getColumns().keySet());
		String sql = String.format("CREATE INDEX %s ON " + tableTemplate()
				+ " (%s)", index.getQuotedName(), index.getTable().getGrain()
				.getName(), index.getTable().getName(), fieldList);
		return sql;
	}

	@Override
	String getDropIndexSQL(Grain g, IndexInfo indexInfo) {
		String sql = String
				.format("DROP INDEX %s ON " + tableTemplate(),
						indexInfo.getIndexName(), g.getName(),
						indexInfo.getTableName());
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
	public ColumnInfo getColumnInfo(Connection conn, Column c)
			throws CelestaException {
		try {
			DatabaseMetaData metaData = conn.getMetaData();
			ResultSet rs = metaData.getColumns(null, c.getParentTable()
					.getGrain().getName(), c.getParentTable().getName(),
					c.getName());
			try {
				if (rs.next()) {
					ColumnInfo result = new ColumnInfo();
					result.setName(rs.getString(COLUMN_NAME));
					String typeName = rs.getString("TYPE_NAME");
					if ("int identity".equalsIgnoreCase(typeName)) {
						result.setType(IntegerColumn.class);
						result.setIdentity(true);
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
}
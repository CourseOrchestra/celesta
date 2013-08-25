package ru.curs.celesta.dbutils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import ru.curs.celesta.CelestaCritical;
import ru.curs.celesta.score.BinaryColumn;
import ru.curs.celesta.score.BooleanColumn;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.DateTimeColumn;
import ru.curs.celesta.score.FloatingColumn;
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
			String getColumnDef(Column c) {
				IntegerColumn ic = (IntegerColumn) c;
				String defaultStr = "";
				// TODO autoincrement
				if (ic.isIdentity()) {
					defaultStr = "IDENTITY";
				} else if (ic.getDefaultValue() != null) {
					defaultStr = DEFAULT + ic.getDefaultValue();
				}
				return join(c.getName(), dbFieldType(), nullable(c), defaultStr);
			}
		});

		TYPES_DICT.put(FloatingColumn.class, new ColumnDefiner() {

			@Override
			String dbFieldType() {
				return "double precision";
			}

			@Override
			String getColumnDef(Column c) {
				FloatingColumn ic = (FloatingColumn) c;
				String defaultStr = "";
				if (ic.getDefaultvalue() != null) {
					defaultStr = DEFAULT + ic.getDefaultvalue();
				}
				return join(c.getName(), dbFieldType(), nullable(c), defaultStr);
			}
		});

		TYPES_DICT.put(StringColumn.class, new ColumnDefiner() {

			@Override
			String dbFieldType() {
				return "varchar";
			}

			@Override
			String getColumnDef(Column c) {
				StringColumn ic = (StringColumn) c;
				String fieldType = String.format("%s(%s)", dbFieldType(),
						ic.isMax() ? "65535" : ic.getLength());
				String defaultStr = "";
				if (ic.getDefaultValue() != null) {
					defaultStr = DEFAULT
							+ StringColumn.quoteString(ic.getDefaultValue());
				}
				return join(c.getName(), fieldType, nullable(c), defaultStr);
			}
		});

		TYPES_DICT.put(BinaryColumn.class, new ColumnDefiner() {

			@Override
			String dbFieldType() {
				return "bytea";
			}

			@Override
			String getColumnDef(Column c) {
				BinaryColumn ic = (BinaryColumn) c;
				String defaultStr = "";
				if (ic.getDefaultValue() != null) {
					defaultStr = DEFAULT + ic.getDefaultValue();
				}
				return join(c.getName(), dbFieldType(), nullable(c), defaultStr);
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
					defaultStr = DEFAULT + "CURRENT_TIMESTAMP";
				} else if (ic.getDefaultValue() != null) {
					defaultStr = DEFAULT + ic.getDefaultValue();
				}
				return join(c.getName(), dbFieldType(), nullable(c), defaultStr);
			}
		});

		TYPES_DICT.put(BooleanColumn.class, new ColumnDefiner() {

			@Override
			String dbFieldType() {
				return "bool";
			}

			@Override
			String getColumnDef(Column c) {
				BooleanColumn ic = (BooleanColumn) c;
				String defaultStr = "";
				if (ic.getDefaultValue() != null) {
					defaultStr = DEFAULT + ic.getDefaultValue();
				}
				return join(c.getName(), dbFieldType(), nullable(c), defaultStr);
			}
		});
	}

	@Override
	boolean tableExists(Connection conn, String schema, String name)
			throws SQLException {
		PreparedStatement check = conn.prepareStatement(String.format(
				"SELECT table_name FROM information_schema.tables  WHERE "
						+ "table_schema = '%s' AND table_name = '%s'", schema,
				name));
		ResultSet rs = check.executeQuery();
		try {
			return rs.next();
		} finally {
			rs.close();
			check.close();
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
		PreparedStatement check = conn
				.prepareStatement(String
						.format("SELECT schema_name FROM information_schema.schemata WHERE schema_name = '%s';",
								name));
		ResultSet rs = check.executeQuery();
		try {
			if (!rs.next()) {
				PreparedStatement create = conn.prepareStatement(String.format(
						"create schema %s;", name));
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
	PreparedStatement getOneRecordStatement(Connection conn, Table t)
			throws CelestaCritical {
		String sql = String.format("select %s from %s.%s where %s limit 1;",
				getTableFieldsList(t), t.getGrain().getName(), t.getName(),
				getRecordWhereClause(t));
		return prepareStatement(conn, sql);
	}

	@Override
	PreparedStatement getRecordSetStatement(Connection conn, Table t,
			Map<String, AbstractFilter> filters, List<String> orderBy)
			throws CelestaCritical {
		// Готовим условие where
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
				throw new RuntimeException("not implemented yet");
		}

		// Соединяем полученные компоненты в стандартный запрос
		// SELECT..FROM..WHERE..ORDER BY
		String sql = getSelectFromOrderBy(t, whereClause.toString(), orderBy);

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
					throw new RuntimeException("not implemented yet");
			}
			return result;
		} catch (SQLException e) {
			throw new CelestaCritical(e.getMessage());
		}
	}

	@Override
	PreparedStatement getInsertRecordStatement(Connection conn, Table t)
			throws CelestaCritical {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < t.getColumns().size(); i++) {
			if (sb.length() > 0)
				sb.append(", ");
			sb.append("?");
		}
		String sql = String.format("insert %s.%s (%s) values (%s);", t
				.getGrain().getName(), t.getName(), getTableFieldsList(t), sb
				.toString());
		return prepareStatement(conn, sql);
	}

	@Override
	PreparedStatement getUpdateRecordStatement(Connection conn, Table t)
			throws CelestaCritical {
		StringBuilder setClause = new StringBuilder();
		for (String c : t.getColumns().keySet()) {
			if (setClause.length() > 0)
				setClause.append(", ");
			setClause.append(String.format("%s = ?", c));
		}

		String sql = String.format("update %s.%s set %s where %s;", t
				.getGrain().getName(), t.getName(), setClause.toString(),
				getRecordWhereClause(t));
		return prepareStatement(conn, sql);
	}

	@Override
	PreparedStatement getDeleteRecordStatement(Connection conn, Table t)
			throws CelestaCritical {
		String sql = String.format("delete %s where %s;", t.getName(),
				getRecordWhereClause(t));
		return prepareStatement(conn, sql);
	}

	@Override
	String getIndicesSQL() {
		return "select * from pg_catalog.pg_indexes where schemaname = '%s';";
	}

	@Override
	String getColumnsSQL() {
		return "select column_name from information_schema.columns "
				+ "where table_schema = '%s' and table_name = '%s';";
	}

}
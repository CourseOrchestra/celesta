package ru.curs.celesta.dbutils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ru.curs.celesta.CelestaCritical;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.score.BinaryColumn;
import ru.curs.celesta.score.BooleanColumn;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.DateTimeColumn;
import ru.curs.celesta.score.FloatingColumn;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.StringColumn;

/**
 * Адаптер MSSQL.
 * 
 */
final class MSSQLAdaptor extends DBAdaptor {

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
				return join(c.getName(), dbFieldType(), nullable(c), defaultStr);
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
				return join(c.getName(), dbFieldType(), nullable(c), defaultStr);
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
				return join(c.getName(), fieldType, nullable(c), defaultStr);
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
				return join(c.getName(), dbFieldType(), nullable(c), defaultStr);
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
					defaultStr = DEFAULT + ic.getDefaultValue();
				}
				return join(c.getName(), dbFieldType(), nullable(c), defaultStr);
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
					defaultStr = DEFAULT + ic.getDefaultValue();
				}
				return join(c.getName(), dbFieldType(), nullable(c), defaultStr);
			}
		});
	}

	@Override
	public boolean tableExists(String schema, String name)
			throws CelestaCritical {
		Connection conn = ConnectionPool.get();
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
			throw new CelestaCritical(e.getMessage());
		} finally {
			ConnectionPool.putBack(conn);
		}
	}

	@Override
	public boolean userTablesExist() throws CelestaCritical {
		Connection conn = ConnectionPool.get();
		try {
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
		} catch (SQLException e) {
			throw new CelestaCritical(e.getMessage());
		} finally {
			ConnectionPool.putBack(conn);
		}
	}

	@Override
	public void createSchemaIfNotExists(String name) throws CelestaCritical {
		Connection conn = ConnectionPool.get();
		try {
			PreparedStatement check = conn.prepareStatement(String.format(
					"select coalesce(SCHEMA_ID('%s'), -1)", name));
			ResultSet rs = check.executeQuery();
			try {
				rs.next();
				if (rs.getInt(1) == -1) {
					PreparedStatement create = conn.prepareStatement(String
							.format("create schema %s;", name));
					create.execute();
					create.close();
				}
			} finally {
				rs.close();
				check.close();
			}
		} catch (SQLException e) {
			throw new CelestaCritical("Cannot create schema. " + e.getMessage());
		} finally {
			ConnectionPool.putBack(conn);
		}
	}

	@Override
	String columnDef(Column c) {
		return TYPES_DICT.get(c.getClass()).getColumnDef(c);
	}

	@Override
	String dbFieldType(Column c) {
		return TYPES_DICT.get(c.getClass()).dbFieldType();
	}
}
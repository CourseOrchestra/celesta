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

	private static final Map<Class<? extends Column>, String> TYPES_DICT = new HashMap<>();
	static {
		TYPES_DICT.put(IntegerColumn.class, "INT");
		TYPES_DICT.put(FloatingColumn.class, "REAL");
		TYPES_DICT.put(StringColumn.class, "NVARCHAR");
		TYPES_DICT.put(BinaryColumn.class, "IMAGE");
		TYPES_DICT.put(DateTimeColumn.class, "DATETIME");
		TYPES_DICT.put(BooleanColumn.class, "BIT");
	}

	@Override
	public boolean tableExists(String schema, String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userTablesExist() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void createSchemaIfNotExists(String name) throws CelestaCritical {
		Connection conn = ConnectionPool.get();
		try {
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
				throw new CelestaCritical(e.getMessage());
			}
		} finally {
			ConnectionPool.putBack(conn);
		}
	}

	@Override
	String dbFieldType(Column c) {
		return TYPES_DICT.get(c.getClass());
	}

}
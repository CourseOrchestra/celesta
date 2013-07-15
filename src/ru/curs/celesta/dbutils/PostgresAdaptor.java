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
 * Адаптер Postgres.
 * 
 */
final class PostgresAdaptor extends DBAdaptor {

	private static final Map<Class<? extends Column>, String> TYPES_DICT = new HashMap<>();
	static {
		TYPES_DICT.put(IntegerColumn.class, "INTEGER");
		TYPES_DICT.put(FloatingColumn.class, "DOUBLE PRECISION");
		TYPES_DICT.put(StringColumn.class, "VARCHAR");
		TYPES_DICT.put(BinaryColumn.class, "BYTEA");
		TYPES_DICT.put(DateTimeColumn.class, "TIMESTAMP");
		TYPES_DICT.put(BooleanColumn.class, "BOOL");
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
				PreparedStatement check = conn
						.prepareStatement(String
								.format("SELECT schema_name FROM information_schema.schemata WHERE schema_name = '%s';",
										name));
				ResultSet rs = check.executeQuery();
				try {
					if (!rs.next()) {
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
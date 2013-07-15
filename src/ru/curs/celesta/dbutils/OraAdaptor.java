package ru.curs.celesta.dbutils;

import java.util.HashMap;
import java.util.Map;

import ru.curs.celesta.CelestaCritical;
import ru.curs.celesta.score.BinaryColumn;
import ru.curs.celesta.score.BooleanColumn;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.DateTimeColumn;
import ru.curs.celesta.score.FloatingColumn;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.StringColumn;

/**
 * Адаптер Ora.
 * 
 */
final class OraAdaptor extends DBAdaptor {

	private static final Map<Class<? extends Column>, String> TYPES_DICT = new HashMap<>();
	static {
		TYPES_DICT.put(IntegerColumn.class, "NUMBER");
		TYPES_DICT.put(FloatingColumn.class, "REAL");
		TYPES_DICT.put(StringColumn.class, "VARCHAR2");
		TYPES_DICT.put(BinaryColumn.class, "BLOB");
		TYPES_DICT.put(DateTimeColumn.class, "TIMESTAMP");
		TYPES_DICT.put(BooleanColumn.class, "CHAR(1)");
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
	public void createSchemaIfNotExists(String string) throws CelestaCritical {
		// TODO Auto-generated method stub

	}
	
	@Override
	String dbFieldType(Column c) {
		return TYPES_DICT.get(c.getClass());
	}

}
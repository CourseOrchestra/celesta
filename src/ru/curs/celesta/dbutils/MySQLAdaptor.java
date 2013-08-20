package ru.curs.celesta.dbutils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ru.curs.celesta.score.BinaryColumn;
import ru.curs.celesta.score.BooleanColumn;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.DateTimeColumn;
import ru.curs.celesta.score.FloatingColumn;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.StringColumn;

/**
 * Адаптер MуSQL.
 */
final class MySQLAdaptor extends DBAdaptor {

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
					defaultStr = "AUTO_INCREMENT";
				} else if (ic.getDefaultValue() != null) {
					defaultStr = DEFAULT + ic.getDefaultValue();
				}
				return join(c.getName(), dbFieldType(), nullable(c), defaultStr);
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
				return join(c.getName(), dbFieldType(), nullable(c), defaultStr);
			}

		}

		);
		TYPES_DICT.put(StringColumn.class, new ColumnDefiner() {

			@Override
			String dbFieldType() {
				return "varchar";
			}

			@Override
			String getColumnDef(Column c) {
				StringColumn ic = (StringColumn) c;
				// See
				// http://stackoverflow.com/questions/332798/equivalent-of-varcharmax-in-mysql
				String fieldType = String.format("%s(%s)", dbFieldType(),
						ic.isMax() ? "21844" : ic.getLength());
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
				return "blob";
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
	boolean tableExists(Connection conn, String schema, String name)
			throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	boolean userTablesExist(Connection conn) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	void createSchemaIfNotExists(Connection conn, String string)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	ColumnDefiner getColumnDefiner(Column c) {
		return TYPES_DICT.get(c.getClass());
	}

}
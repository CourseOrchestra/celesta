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
 * Адаптер Ora.
 * 
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
				// TODO autoincrement
				if (ic.isIdentity()) {
					defaultStr = "IDENTITY";
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
				return join(c.getName(), dbFieldType(), nullable(c), defaultStr);
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
					defaultStr = DEFAULT + ic.getDefaultValue();
				}
				// TODO: constraint на Y/N
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
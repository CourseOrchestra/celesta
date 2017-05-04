package ru.curs.lyra;

import java.util.HashMap;

import ru.curs.celesta.score.BinaryColumn;
import ru.curs.celesta.score.BooleanColumn;
import ru.curs.celesta.score.ColumnMeta;
import ru.curs.celesta.score.DateTimeColumn;
import ru.curs.celesta.score.FloatingColumn;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.StringColumn;

/**
 * Тип сериализуемого поля формы.
 */
public enum LyraFieldType {
	/**
	 * BLOB.
	 */
	BLOB,

	/**
	 * BIT.
	 */
	BIT,

	/**
	 * DATETIME.
	 */
	DATETIME,

	/**
	 * REAL.
	 */
	REAL,

	/**
	 * INT.
	 */
	INT,

	/**
	 * VARCHAR.
	 */
	VARCHAR;

	private static final HashMap<String, LyraFieldType> C2L = new HashMap<>();

	static {
		C2L.put(IntegerColumn.CELESTA_TYPE, INT);
		C2L.put(StringColumn.VARCHAR, VARCHAR);
		C2L.put(StringColumn.TEXT, VARCHAR);
		C2L.put(FloatingColumn.CELESTA_TYPE, REAL);
		C2L.put(DateTimeColumn.CELESTA_TYPE, DATETIME);
		C2L.put(BooleanColumn.CELESTA_TYPE, BIT);
		C2L.put(BinaryColumn.CELESTA_TYPE, BLOB);
	}

	/**
	 * Определяет тип поля по метаданным столбца таблицы (Table).
	 * 
	 * @param c
	 *            столбец таблицы.
	 * 
	 */
	public static LyraFieldType lookupFieldType(ColumnMeta c) {
		LyraFieldType result = C2L.get(c.getCelestaType());
		if (result == null) {
			throw new RuntimeException(String.format("Invalid table column type: %s", c.getClass().toString()));
		}
		return result;
	}

}

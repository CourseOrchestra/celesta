package ru.curs.lyra;

import ru.curs.celesta.score.BinaryColumn;
import ru.curs.celesta.score.BooleanColumn;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.DateTimeColumn;
import ru.curs.celesta.score.FloatingColumn;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.StringColumn;
import ru.curs.celesta.score.ViewColumnType;

/**
 * Тип сериализуемого поля формы.
 */
public enum LyraFieldType {
	/**
	 * BLOB.
	 */
	BLOB, /**
	 * BIT.
	 */
	BIT, /**
	 * DATETIME.
	 */
	DATETIME, /**
	 * REAL.
	 */
	REAL, /**
	 * INT.
	 */
	INT, /**
	 * VARCHAR.
	 */
	VARCHAR;

	/**
	 * Определяет тип поля по метаданным столбца таблицы (Table).
	 * 
	 * @param c
	 *            столбец таблицы.
	 * 
	 */
	public static LyraFieldType lookupFieldType(Column c) {
		if (c instanceof IntegerColumn) {
			return INT;
		} else if (c instanceof StringColumn) {
			return VARCHAR;
		} else if (c instanceof FloatingColumn) {
			return REAL;
		} else if (c instanceof DateTimeColumn) {
			return DATETIME;
		} else if (c instanceof BooleanColumn) {
			return BIT;
		} else if (c instanceof BinaryColumn) {
			return BLOB;
		} else {
			throw new RuntimeException(String.format(
					"Invalid table column type: %s", c.getClass().toString()));
		}
	}

	/**
	 * Определяет тип поля по типу столбца представления (View).
	 * 
	 * @param c
	 *            тип столбца представления.
	 * */
	public static LyraFieldType lookupFieldType(ViewColumnType c) {
		switch (c) {
		case NUMERIC:
			return REAL;
		case TEXT:
			return VARCHAR;
		case DATE:
			return DATETIME;
		case BIT:
			return BIT;
		case BLOB:
			return BLOB;
		default:
			throw new RuntimeException(String.format(
					"Invalid view column type: %s", c.toString()));
		}
	}

}

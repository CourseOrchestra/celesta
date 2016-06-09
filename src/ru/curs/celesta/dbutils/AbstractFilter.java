package ru.curs.celesta.dbutils;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.filter.FilterParser;
import ru.curs.celesta.dbutils.filter.FilterParser.FilterType;
import ru.curs.celesta.score.DateTimeColumn;
import ru.curs.celesta.score.FloatingColumn;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.StringColumn;
import ru.curs.celesta.score.ViewColumnType;

/**
 * Внутреннее представление фильтра на поле.
 */
abstract class AbstractFilter {

}

/**
 * Фильтр в виде единичного значения.
 */
class SingleValue extends AbstractFilter {
	private final Object value;

	public SingleValue(Object value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return String.format("%s", value);
	}

	public Object getValue() {
		return value;
	}
}

/**
 * Фильтр в виде диапазона значений от..до.
 * 
 */
class Range extends AbstractFilter {
	private final Object valueFrom;
	private final Object valueTo;

	public Range(Object valueFrom, Object valueTo) {
		this.valueFrom = valueFrom;
		this.valueTo = valueTo;
	}

	@Override
	public String toString() {
		return String.format("%s..%s", valueFrom.toString(), valueTo.toString());
	}

	public Object getValueFrom() {
		return valueFrom;
	}

	public Object getValueTo() {
		return valueTo;
	}
}

/**
 * Фильтр по одному полю.
 */
class Filter extends AbstractFilter {

	private final String value;

	public Filter(String value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return String.format("%s", value);
	}

	public String makeWhereClause(String quotedName, Object c, final DBAdaptor dba) throws CelestaException {
		FilterType ft;
		if (c instanceof IntegerColumn || c instanceof FloatingColumn || c == ViewColumnType.INT
				|| c == ViewColumnType.REAL)
			ft = FilterType.NUMERIC;
		else if (c instanceof DateTimeColumn || c == ViewColumnType.DATE)
			ft = FilterType.DATETIME;
		else if (c instanceof StringColumn || c == ViewColumnType.TEXT)
			ft = FilterType.TEXT;
		else {
			ft = FilterType.OTHER;
		}

		FilterParser.SQLTranslator tr = new FilterParser.SQLTranslator() {
			@Override
			public String translateDate(String date) throws CelestaException {
				return dba.translateDate(date);
			}

		};
		String result = FilterParser.translateFilter(ft, quotedName, value, tr);
		return result;
	}
}
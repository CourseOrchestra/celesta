package ru.curs.celesta.dbutils;

import java.util.HashMap;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.filter.FilterParser;
import ru.curs.celesta.dbutils.filter.FilterParser.FilterType;
import ru.curs.celesta.score.ColumnMeta;
import ru.curs.celesta.score.DateTimeColumn;
import ru.curs.celesta.score.FloatingColumn;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.StringColumn;

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

	private final static HashMap<String, FilterType> C2F = new HashMap<>();

	static {
		C2F.put(IntegerColumn.CELESTA_TYPE, FilterType.NUMERIC);
		C2F.put(FloatingColumn.CELESTA_TYPE, FilterType.NUMERIC);
		C2F.put(DateTimeColumn.CELESTA_TYPE, FilterType.DATETIME);
		C2F.put(StringColumn.VARCHAR, FilterType.TEXT);
		C2F.put(StringColumn.TEXT, FilterType.TEXT);
	}

	public Filter(String value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return String.format("%s", value);
	}

	public String makeWhereClause(String quotedName, ColumnMeta c, final DBAdaptor dba) throws CelestaException {
		FilterType ft = C2F.getOrDefault(c.getCelestaType(), FilterType.OTHER);

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
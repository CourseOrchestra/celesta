package ru.curs.celesta.dbutils.filter;

import ru.curs.celesta.dbutils.QueryBuildingHelper;
import ru.curs.celesta.score.*;

import java.util.HashMap;

/**
 * Single field filter.
 */
public final class Filter extends AbstractFilter {

    private static final HashMap<String, FilterParser.FilterType> C2F = new HashMap<>();

    static {
        C2F.put(IntegerColumn.CELESTA_TYPE, FilterParser.FilterType.NUMERIC);
        C2F.put(FloatingColumn.CELESTA_TYPE, FilterParser.FilterType.NUMERIC);
        C2F.put(DateTimeColumn.CELESTA_TYPE, FilterParser.FilterType.DATETIME);
        C2F.put(StringColumn.VARCHAR, FilterParser.FilterType.TEXT);
        C2F.put(StringColumn.TEXT, FilterParser.FilterType.TEXT);
    }

    private final String value;
    private final FilterParser.FilterType ftype;

    public Filter(String value, ColumnMeta<?> c) {
        this.value = value;
        this.ftype = C2F.getOrDefault(c.getCelestaType(), FilterParser.FilterType.OTHER);
    }

    @Override
    public String toString() {
        return value;
    }

    public String makeWhereClause(String quotedName, final QueryBuildingHelper dba) {
        String result = FilterParser.translateFilter(ftype, quotedName, value, dba::translateDate);
        return result;
    }

    @Override
    public boolean filterEquals(AbstractFilter f) {
        if (f instanceof Filter) {
            Object v2 = ((Filter) f).value;
            return value == null ? v2 == null : value.equals(v2);
        } else {
            return false;
        }
    }
}

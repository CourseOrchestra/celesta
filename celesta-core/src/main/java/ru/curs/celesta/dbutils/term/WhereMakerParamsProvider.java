package ru.curs.celesta.dbutils.term;

import ru.curs.celesta.dbutils.QueryBuildingHelper;
import ru.curs.celesta.dbutils.filter.AbstractFilter;
import ru.curs.celesta.dbutils.filter.In;
import ru.curs.celesta.score.Expr;

import java.util.Map;

/**
 * The interface that provides needed information for building filter/navigation
 * queries. This could be Cursor, but we extracted this interface for
 * testability.
 */
public interface WhereMakerParamsProvider {
    QueryBuildingHelper dba();

    void initOrderBy();

    String[] sortFields();

    int[] sortFieldsIndices();

    boolean[] descOrders();

    Object[] values();

    boolean isNullable(String columnName);

    Map<String, AbstractFilter> filters();

    Expr complexFilter();

    In inFilter();
}

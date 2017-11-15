package ru.curs.celesta.dbutils.term;

import ru.curs.celesta.CelestaException;
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

	void initOrderBy() throws CelestaException;

	String[] sortFields() throws CelestaException;

	int[] sortFieldsIndices() throws CelestaException;

	boolean[] descOrders() throws CelestaException;

	Object[] values() throws CelestaException;

	boolean isNullable(String columnName) throws CelestaException;

	Map<String, AbstractFilter> filters();

	Expr complexFilter();

	In inFilter();
}

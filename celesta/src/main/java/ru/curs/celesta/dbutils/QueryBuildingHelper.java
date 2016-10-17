package ru.curs.celesta.dbutils;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.SQLGenerator;

/**
 * Subset of DBAdaptor functions for literals translation.
 */
public interface QueryBuildingHelper {

	/**
	 * Translates date from ISO YYYYMMDD format into RDBMS-specific format.
	 * 
	 * @param date
	 *            date in ISO format.
	 * @throws CelestaException
	 *             wrong format.
	 */
	String translateDate(String date) throws CelestaException;

	/**
	 * Does RDBMS sort nulls first?
	 */
	boolean nullsFirst();

	/**
	 * Returns SQL generator for Celesta views/complex filters.
	 */
	SQLGenerator getViewSQLGenerator();
}
package ru.curs.celesta.dbutils;

import ru.curs.celesta.score.DataGrainElement;
import ru.curs.celesta.score.SQLGenerator;

import java.sql.Connection;
import java.time.ZonedDateTime;
import java.util.List;

/**
 * Subset of DBAdaptor functions for literals translation.
 */
public interface QueryBuildingHelper {

    /**
     * Translates date from ISO YYYYMMDD format into RDBMS-specific format.
     *
     * @param date
     *            date in ISO format.
     * @
     *             wrong format.
     */
    String translateDate(String date) ;

    /**
     * Does RDBMS sort nulls first?
     */
    boolean nullsFirst();

    /**
     * Returns SQL generator for Celesta views/complex filters.
     */
    SQLGenerator getViewSQLGenerator();

    //TODO: Javadoc
    String getInFilterClause(DataGrainElement dge, DataGrainElement otherDge, List<String> fields,
                             List<String> otherFields, String whereForOtherTable);

    //TODO: Javadoc
    boolean supportsCortegeComparing();

    ZonedDateTime prepareZonedDateTimeForParameterSetter(Connection conn, ZonedDateTime z);
}

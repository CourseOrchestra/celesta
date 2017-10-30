package ru.curs.celesta.dbutils.adaptors;

import ru.curs.celesta.CelestaException;

import java.util.List;

public interface StaticDataAdaptor {

    /**
     * Selects list of static strings in specified order
     * @param data List of String to select
     * @param columnName name of result column
     * @param orderBy expression to concat after "ORDER BY"
     * @return {@link List<String> }
     * @throws CelestaException
     */
    List<String> selectStaticStrings(
            List<String> data, String columnName, String orderBy
    ) throws CelestaException;

    /**
     * Compares strings by db
     * @param left
     * @param right
     * @return -1 if left < right, 0 if left == right, 1 if left > right
     * @throws CelestaException
     */
    int compareStrings(String left, String right) throws CelestaException;
}

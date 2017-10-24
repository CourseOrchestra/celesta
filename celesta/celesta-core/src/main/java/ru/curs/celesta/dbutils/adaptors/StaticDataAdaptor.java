package ru.curs.celesta.dbutils.adaptors;

import ru.curs.celesta.CelestaException;

import java.util.List;

public interface StaticDataAdaptor {
    //TODO!!! Javadoc
    List<String> selectStaticStrings(
            List<String> data, String columnName, String orderType
    ) throws CelestaException;

    int compareStrings(String left, String right) throws CelestaException;
}

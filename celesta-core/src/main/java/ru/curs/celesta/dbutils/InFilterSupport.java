package ru.curs.celesta.dbutils;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.filter.In;
import ru.curs.celesta.dbutils.filter.value.FieldsLookup;

public interface InFilterSupport {

    FieldsLookup setIn(BasicCursor otherCursor) throws CelestaException;
    In getIn();
}

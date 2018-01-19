package ru.curs.celesta.dbutils.term;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.Table;
import ru.curs.celesta.score.TableElement;

public class CsqlWhereTermsMaker {

    public static WhereTerm getPKWhereTermForGet(TableElement t) throws CelestaException {
        WhereTerm r = null;
        int i = 0;
        for (String colName : t.getPrimaryKey().keySet()) {
            WhereTerm l = new FieldCompTerm("\"" + colName + "\"", i++, "=");
            r = r == null ? l : AndTerm.construct(l, r);
        }
        return r == null ? AlwaysTrue.TRUE : r;
    }

    /**
     * Gets WHERE clause for single record (by its primary key).
     *
     * @param t
     *            Table meta.
     * @throws CelestaException
     *             Celesta error.
     */
    public static WhereTerm getPKWhereTerm(Table t) throws CelestaException {
        WhereTerm r = null;
        for (String colName : t.getPrimaryKey().keySet()) {
            WhereTerm l = new FieldCompTerm("\"" + colName + "\"", t.getColumnIndex(colName), "=");
            r = r == null ? l : AndTerm.construct(l, r);
        }
        return r == null ? AlwaysTrue.TRUE : r;
    }
}

package ru.curs.celesta.score;

public class AbstractViewUtils {

    public static void addColumn(AbstractView v, String alias, Expr expr) throws ParseException {
        v.addColumn(alias, expr);
    }

    public static void addFromTableRef(AbstractView v, TableRef ref) throws ParseException {
        v.addFromTableRef(ref);
    }

    public static void addGroupByColumn(AbstractView v, FieldRef fr) throws ParseException {
        v.addGroupByColumn(fr);
    }

    public static void finalizeParsing(AbstractView v) throws ParseException {
        v.finalizeParsing();
    }
}

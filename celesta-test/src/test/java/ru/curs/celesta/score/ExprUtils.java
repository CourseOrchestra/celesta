package ru.curs.celesta.score;

public class ExprUtils {

    public static FieldRef fieldRef(String tableNameOrAlias, String columnName) throws ParseException {
        return new FieldRef(tableNameOrAlias, columnName);
    }

    public static Sum sum(Expr term) {
        return new Sum(term);
    }
}

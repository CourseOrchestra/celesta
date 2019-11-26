package ru.curs.celesta.dbutils.adaptors.function;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.meta.DbColumnInfo;
import ru.curs.celesta.score.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Utility class for Oracle functions.
 */
public final class OraFunctions {

    private OraFunctions() {
        throw new AssertionError();
    }

    public static String getBooleanCheckName(Column<?> c) {
        String result = String.format("chk_%s_%s_%s", c.getParentTable().getGrain().getName(),
                c.getParentTable().getName(), c.getName());
        result = NamedElement.limitName(result);
        return "\"" + result + "\"";
    }

    public static String translateDate(String date)  {
        try {
            Date d = DateTimeColumn.parseISODate(date);
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            return String.format("date '%s'", df.format(d));
        } catch (ParseException e) {
            throw new CelestaException(e.getMessage());
        }

    }

    public static boolean fromOrToNClob(Column<?> c, DbColumnInfo actual) {
        return (actual.isMax() || isNclob(c)) && !(actual.isMax() && isNclob(c));
    }

    private static boolean isNclob(Column<?> c) {
        return c instanceof StringColumn && ((StringColumn) c).isMax();
    }

}

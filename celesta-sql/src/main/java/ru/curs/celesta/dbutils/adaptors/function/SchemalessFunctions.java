package ru.curs.celesta.dbutils.adaptors.function;

import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.NamedElement;
import ru.curs.celesta.score.TableElement;

/**
 * Utility class for schemaless database functions.
 */
public final class SchemalessFunctions {

    private SchemalessFunctions() {
        throw new AssertionError();
    }

    public static String getIncrementSequenceName(TableElement table) {
        String result = String.format("%s_%s_inc", table.getGrain().getName(), table.getName());
        result = NamedElement.limitName(result);
        return result;
    }

    public static String getUpdTriggerName(TableElement table) {
        String result = String.format("%s_%s_upd", table.getGrain().getName(), table.getName());
        result = NamedElement.limitName(result);
        return result;
    }

    public static String generateSequenceTriggerName(IntegerColumn ic) {
        TableElement te = ic.getParentTable();
        String result = String.format("%s_%s_%s_seq", te.getGrain().getName(), te.getName(), ic.getName());
        return NamedElement.limitName(result);
    }

    public static String getVersionCheckTriggerName(TableElement table) {
        String result = String.format("%s_%s_ver", table.getGrain().getName(), table.getName());
        result = NamedElement.limitName(result);
        return result;
    }

}

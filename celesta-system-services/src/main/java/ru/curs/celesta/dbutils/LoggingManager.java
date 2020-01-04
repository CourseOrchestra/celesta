package ru.curs.celesta.dbutils;

import ru.curs.celesta.*;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.score.BasicTable;
import ru.curs.celesta.syscursors.LogCursor;
import ru.curs.celesta.syscursors.LogsetupCursor;

/**
 * Logging manager. Writes to log changed values (if needed).
 */
public final class LoggingManager implements ILoggingManager {
    /**
     * Cache size (in number of entries). MUST BE POWER OF TWO!! This cache may be
     * smaller than access rights system cache, because it stores entries
     * at the level of a table and not at the level of combination "user, table".
     */
    private static final int CACHE_SIZE = 1024;
    /**
     * "Shelf life" of a cache entry (in milliseconds).
     */
    private static final int CACHE_ENTRY_SHELF_LIFE = 20000;

    private final ICelesta celesta;
    private final DBAdaptor dbAdaptor;

    private CacheEntry[] cache = new CacheEntry[CACHE_SIZE];

    /**
     * Entry of the internal cache.
     */
    private static class CacheEntry {
        private final BasicTable table;
        private final int loggingMask;
        private final long expirationTime;

        CacheEntry(BasicTable table, int loggingMask) {
            this.table = table;
            this.loggingMask = loggingMask;
            expirationTime = System.currentTimeMillis()
                    + CACHE_ENTRY_SHELF_LIFE;

        }

        public static int hash(BasicTable table) {
            return (table.getGrain().getName() + '|' + table.getName())
                    .hashCode();
        }

        public boolean isExpired() {
            return System.currentTimeMillis() > expirationTime;
        }

        public boolean isLoggingNeeded(Action a) {
            if (a == Action.READ) {
                throw new IllegalArgumentException();
            }
            return (loggingMask & a.getMask()) != 0;
        }

    }

    public LoggingManager(ICelesta celesta, DBAdaptor dbAdaptor) {
        this.celesta = celesta;
        this.dbAdaptor = dbAdaptor;
    }

    boolean isLoggingNeeded(CallContext sysContext, BasicTable t, Action a) {
        // Calculate the location of data in the cache.
        int index = CacheEntry.hash(t) & (CACHE_SIZE - 1);

        // First of all look up if there is a suitable non-stale entry
        // (otherwise - update the cache).
        CacheEntry ce = cache[index];
        if (ce == null || ce.isExpired() || ce.table != t) {
            ce = refreshLogging(sysContext, t);
            cache[index] = ce;
        }
        return ce.isLoggingNeeded(a);
    }

    private CacheEntry refreshLogging(CallContext sysContext, BasicTable t) {
        LogsetupCursor logsetup = new LogsetupCursor(sysContext);
        int loggingMask = 0;
        if (logsetup.tryGet(t.getGrain().getName(), t.getName())) {
            loggingMask |= logsetup.getI() ? Action.INSERT.getMask() : 0;
            loggingMask |= logsetup.getM() ? Action.MODIFY.getMask() : 0;
            loggingMask |= logsetup.getD() ? Action.DELETE.getMask() : 0;
        }
        return new CacheEntry(t, loggingMask);
    }

    /**
     * Log an action on cursor.
     * <p>
     * Cursors from <b>celesta</b> grain will be ignored.
     *
     * @param c  cursor
     * @param a  action
     */
    public void log(Cursor c, Action a) {
        if (a == Action.READ) {
            throw new IllegalArgumentException();
        }
        // No logging for celesta.grains (this is needed for smooth update from
        // versions having no recversion fields).
        if ("celesta".equals(c.meta().getGrain().getName())
                && ("grains".equals(c.meta().getName())
                || "tables".equals(c.meta().getName()))) {
            return;
        }

        try (CallContext sysContext = new SystemCallContext(celesta, "log")) {
            if (!isLoggingNeeded(sysContext, c.meta(), a)) {
                return;
            }
            writeToLog(c, a, sysContext);
        }
    }

    private void writeToLog(Cursor c, Action a, CallContext sysContext) {
        LogCursor log = new LogCursor(sysContext);
        log.init();
        log.setUserid(c.callContext().getUserId());
        log.setGrainid(c.meta().getGrain().getName());
        log.setTablename(c._objectName());
        log.setAction_type(a.shortId());
        Object[] o = c._currentKeyValues();

        String value;
        int len;
        if (o.length > 0) {
            value = o[0] == null ? "NULL" : o[0].toString();
            len = log.getMaxStrLen(log.COLUMNS.pkvalue1());
            log.setPkvalue1(trimValue(value, len));
        }
        if (o.length > 1) {
            value = o[1] == null ? "NULL" : o[1].toString();
            len = log.getMaxStrLen(log.COLUMNS.pkvalue2());
            log.setPkvalue2(trimValue(value, len));
        }
        if (o.length > 2) {
            value = o[2] == null ? "NULL" : o[2].toString();
            len = log.getMaxStrLen(log.COLUMNS.pkvalue3());
            log.setPkvalue3(trimValue(value, len));
        }

        len = log.getMaxStrLen(log.COLUMNS.newvalues());
        switch (a) {
            case INSERT:
                value = c.asCSVLine();
                log.setNewvalues(trimValue(value, len));
                break;
            case MODIFY:
                value = c.asCSVLine();
                log.setNewvalues(trimValue(value, len));

                value = c.getXRec().asCSVLine();
                log.setOldvalues(trimValue(value, len));
                break;
            case DELETE:
                value = c.getXRec().asCSVLine();
                log.setOldvalues(trimValue(value, len));
                break;
            default:
        }
        log.insert();
    }

    private static String trimValue(String value, int len) {
        return value.length() > len ? value.substring(0, len) : value;
    }

}

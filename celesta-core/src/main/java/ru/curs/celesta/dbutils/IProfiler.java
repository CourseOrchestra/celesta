package ru.curs.celesta.dbutils;

import ru.curs.celesta.CallContext;

public interface IProfiler {
    /**
     * Procedures with this name will never be logged in profiler.
     * <p>
     * This is applicable first of all for the profiler itself to avoid
     * infinite loop, then to DBUpdater calls since there can be
     * no table to write the log yet.
     */
    String NO_LOG = "NO_LOG";

    void logCall(CallContext context);
}

package ru.curs.celesta;

import ru.curs.celesta.dbutils.BasicDataAccessor;
import ru.curs.celesta.dbutils.ILoggingManager;
import ru.curs.celesta.dbutils.IPermissionManager;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.score.Score;

import java.sql.Connection;
import java.sql.SQLException;

import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;

/**
 * Call context containing a DB connection carrying a transaction and a user identifier.
 */
public class CallContext implements ICallContext {

    private enum State {
        NEW,
        ACTIVE,
        CLOSED
    }

    /**
     * Maximal number of accessors that can be opened within single context.
     */
    public static final int MAX_DATA_ACCESSORS = 1023;

    private static final Map<Connection, Integer> PIDSCACHE = Collections
            .synchronizedMap(new WeakHashMap<>());

    private final String userId;

    private ICelesta celesta;
    private Connection conn;
    private String procName;

    private int dbPid;
    private Date startTime;
    private long activationTime;
    private long closingTime;

    private BasicDataAccessor lastDataAccessor;

    private int dataAccessorsCount;
    private State state;

    /**
     * Creates new not activated context.
     *
     * @param userId User identifier. Cannot be null or empty.
     */
    public CallContext(String userId) {
        if (Objects.requireNonNull(userId).isEmpty()) {
            throw new CelestaException("Call context's user Id must not be empty");
        }
        this.userId = userId;
        state = State.NEW;
    }

    /**
     * Creates activated context.
     *
     * @param userId   User identifier. Cannot be null or empty.
     * @param celesta  Celesta instance.
     * @param procName Procedure which is being called in this context.
     */
    public CallContext(String userId, ICelesta celesta, String procName) {
        this(userId);
        activate(celesta, procName);
    }

    final int getDataAccessorsCount() {
        return dataAccessorsCount;
    }

    /**
     * Activates CallContext with 'live' Celesta and procName.
     *
     * @param celesta  Celesta to use CallContext with.
     * @param procName Name of the called procedure (for logging/audit needs).
     */
    public void activate(ICelesta celesta, String procName) {
        Objects.requireNonNull(celesta);
        Objects.requireNonNull(procName);

        if (state != State.NEW) {
            throw new CelestaException("Cannot activate CallContext in %s state (NEW expected).", state);
        }
        this.celesta = celesta;
        this.procName = procName;
        this.state = State.ACTIVE;
        conn = celesta.getConnectionPool().get();
        dbPid = PIDSCACHE.computeIfAbsent(conn,
                getDbAdaptor()::getDBPid);
        startTime = new Date();
        activationTime = System.nanoTime();
    }

    /**
     * Returns active database JDBC connection.
     *
     * @return
     */
    public Connection getConn() {
        return conn;
    }

    /**
     * Returns name of the current user.
     *
     * @return
     */
    public String getUserId() {
        return userId;
    }

    /**
     * Commits the current transaction. Will cause error for not-activated or closed context.
     * <p>
     * Wraps SQLException into CelestaException.
     */
    public void commit() {
        if (state == State.ACTIVE) {
            try {
                conn.commit();
            } catch (SQLException e) {
                throw new CelestaException(
                        String.format("Commit unsuccessful: %s", e.getMessage()), e);
            }
        } else {
            throw new CelestaException("Not active context cannot be committed");
        }
    }

    /**
     * Rollbacks the current transaction. Does nothing for not-activated context.
     * <p>
     * Wraps SQLException into CelestaException.
     */
    public void rollback() {
        if (conn != null) {
            try {
                conn.rollback();
            } catch (SQLException e) {
                throw new CelestaException(
                        String.format("Rollback unsuccessful: %s", e.getMessage()), e);
            }
        }
    }

    /**
     * Returns Celesta instance.
     *
     * @return {@code null} for not activated context
     */
    public ICelesta getCelesta() {
        return celesta;
    }

    /**
     * Returns score of current Celesta instance.
     *
     * @return
     */
    public Score getScore() {
        return celesta.getScore();
    }

    /**
     * Set the last data accessor object.
     *
     * @param dataAccessor  data accessor object
     */
    public void setLastDataAccessor(BasicDataAccessor dataAccessor) {
        lastDataAccessor = dataAccessor;
    }

    /**
     * Increments counter of open data accessor objects.
     *
     * @throws CelestaException  if the maximal limit of data accessors is exceeded.
     */
    public void incDataAccessorsCount() {
        if (dataAccessorsCount > MAX_DATA_ACCESSORS) {
            throw new CelestaException(
                    "Too many data accessors created in one Celesta procedure call. Check for leaks!"
            );
        }
        dataAccessorsCount++;
    }

    /**
     * Decrements counter of open data accessor objects.
     */
    public void decDataAccessorsCount() {
        dataAccessorsCount--;
    }

    /**
     * Returns the last data accessor object.
     *
     * @return
     */
    public BasicDataAccessor getLastDataAccessor() {
        return lastDataAccessor;
    }

    /**
     * Closes all data accessor classes.
     */
    private void closeDataAccessors() {
        while (lastDataAccessor != null) {
            lastDataAccessor.close();
        }
    }

    /**
     * Returns Process Id of current connection to the database.
     *
     * @return
     */
    public int getDBPid() {
        return dbPid;
    }

    /**
     * Returns procedure name that was initially called.
     *
     * @return
     */
    public String getProcName() {
        return procName;
    }

    /**
     * Returns the calendar date of CallContext activation.
     *
     * @return
     */
    public Date getStartTime() {
        return startTime;
    }

    /**
     * Returns number of nanoseconds since CallContext activation.
     *
     * @return
     */
    public long getDurationNs() {
        switch (state) {
            case NEW:
                return 0;
            case ACTIVE:
                return System.nanoTime() - activationTime;
            default:
                return closingTime - activationTime;
        }

    }

    /**
     * If this context is closed.
     *
     * @return
     */
    public boolean isClosed() {
        return state == State.CLOSED;
    }

    /**
     * Returns a {@link IPermissionManager} of current Celesta instance.
     *
     * @return
     */
    public IPermissionManager getPermissionManager() {
        return celesta.getPermissionManager();
    }

    /**
     * Returns a {@link ILoggingManager} of current Celesta instance.
     *
     * @return
     */
    public ILoggingManager getLoggingManager() {
        return celesta.getLoggingManager();
    }

    /**
     * Returns a {@link DBAdaptor} of current Celesta instance.
     *
     * @return
     */
    public DBAdaptor getDbAdaptor() {
        return celesta.getDBAdaptor();
    }

    @Override
    public final void close() {
        try {
            closeDataAccessors();
            if (conn != null) {
                conn.close();
            }
            if (celesta != null) {
                celesta.getProfiler().logCall(this);
            }
            closingTime = System.nanoTime();
            state = State.CLOSED;
        } catch (Exception e) {
            throw new CelestaException("Can't close callContext", e);
        }
    }

    /**
     * Duplicates callcontext with another JDBC connection.
     *
     * @return
     */
    public CallContext getCopy() {
        CallContext cc = new CallContext(userId);
        cc.activate(celesta, procName);
        return cc;
    }

}

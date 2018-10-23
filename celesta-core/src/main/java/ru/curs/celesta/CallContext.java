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
 * Контекст вызова, содержащий несущее транзакцию соединение с БД и
 * идентификатор пользователя.
 */
public class CallContext implements ICallContext {

    private enum State {
        NEW,
        ACTIVE,
        CLOSED
    }

    /**
     * Максимальное число объектов доступа, которое может быть открыто в одном
     * контексте.
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
    private long startMonotonicTime;

    private BasicDataAccessor lastDataAccessor;

    private int dataAccessorsCount;
    private State state;

    public CallContext(String userId) {
        if (Objects.requireNonNull(userId).isEmpty()) {
            throw new CelestaException("Call context's user Id must not be empty");
        }
        this.userId = userId;
        state = State.NEW;
    }

    public CallContext(String userId, ICelesta celesta, String procName) {
        this(userId);
        activate(celesta, procName);
    }

    int getDataAccessorsCount() {
        return dataAccessorsCount;
    }

    public void activate(ICelesta celesta,
                         String procName) {
        Objects.requireNonNull(celesta);
        Objects.requireNonNull(procName);
        if (state != State.NEW)
            throw new CelestaException("Cannot activate CallContext in %s state (NEW expected).",
                    state);

        this.celesta = celesta;
        this.state = State.ACTIVE;
        this.procName = procName;
        conn = celesta.getConnectionPool().get();
        dbPid = PIDSCACHE.computeIfAbsent(conn,
                getDbAdaptor()::getDBPid);
        startTime = new Date();
        startMonotonicTime = System.nanoTime();
    }

    public Connection getConn() {
        return conn;
    }

    public String getUserId() {
        return userId;
    }

    /**
     * Commits the current transaction.
     * <p>
     * Wraps SQLException into CelestaException.
     */
    public void commit() {
        try {
            conn.commit();
        } catch (SQLException e) {
            throw new CelestaException("Commit unsuccessful: %s", e.getMessage());
        }
    }

    /**
     * Rollbacks the current transaction.
     * <p>
     * Wraps SQLException into CelestaException.
     */
    public void rollback() {
        try {
            conn.rollback();
        } catch (SQLException e) {
            throw new CelestaException("Rollback unsuccessful: %s", e.getMessage());
        }
    }

    public ICelesta getCelesta() {
        return celesta;
    }

    public Score getScore() {
        return celesta.getScore();
    }

    public void setLastDataAccessor(BasicDataAccessor dataAccessor) {
        lastDataAccessor = dataAccessor;
    }

    public void incDataAccessorsCount() {
        if (dataAccessorsCount > MAX_DATA_ACCESSORS)
            throw new CelestaException("Too many data accessors created in one Celesta procedure call. Check for leaks!");
        dataAccessorsCount++;
    }

    /**
     * Уменьшает счетчик открытых объектов доступа.
     */
    public void decDataAccessorsCount() {
        dataAccessorsCount--;
    }

    /**
     * Получает последний объект доступа.
     */
    public BasicDataAccessor getLastDataAccessor() {
        return lastDataAccessor;
    }

    /**
     * Закрытие всех классов доступа.
     */
    private void closeDataAccessors() {
        while (lastDataAccessor != null) {
            lastDataAccessor.close();
        }
    }

    /**
     * Возвращает Process Id текущего подключения к базе данных.
     */
    public int getDBPid() {
        return dbPid;
    }

    /**
     * Возвращает имя процедуры, которая была изначально вызвана.
     */
    public String getProcName() {
        return procName;
    }

    /**
     * Returns the calendar date of CallContext activation.
     */
    public Date getStartTime() {
        return startTime;
    }

    /**
     * Returns number of nanoseconds since CallContext activation.
     */
    public long getDurationNs() {
        return System.nanoTime() - startMonotonicTime;
    }

    public boolean isClosed() {
        return state == State.CLOSED;
    }

    public IPermissionManager getPermissionManager() {
        return celesta.getPermissionManager();
    }

    public ILoggingManager getLoggingManager() {
        return celesta.getLoggingManager();
    }

    public DBAdaptor getDbAdaptor() {
        return celesta.getDBAdaptor();
    }

    @Override
    public void close() {
        try {
            closeDataAccessors();
            if (conn != null)
                conn.close();
            celesta.getProfiler().logCall(this);
            state = State.CLOSED;
        } catch (Exception e) {
            throw new CelestaException("Can't close callContext", e);
        }
    }

    /**
     * Duplicates callcontext with another JDBC connection.
     */
    public CallContext getCopy() {
        CallContext cc = new CallContext(userId);
        cc.activate(celesta, procName);
        return cc;
    }

}

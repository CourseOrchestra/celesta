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
import java.util.WeakHashMap;

/**
 * Контекст вызова, содержащий несущее транзакцию соединение с БД и
 * идентификатор пользователя.
 */
public class CallContext implements ICallContext {

    /**
     * Максимальное число объектов доступа, которое может быть открыто в одном
     * контексте.
     */
    public static final int MAX_DATA_ACCESSORS = 1023;

    private static final Map<Connection, Integer> PIDSCACHE = Collections
            .synchronizedMap(new WeakHashMap<>());
    protected final ICelesta celesta;
    private final Connection conn;
    protected final String procName;
    private final String userId;
    private final int dbPid;
    private final Date startTime = new Date();

    private BasicDataAccessor lastDataAccessor;

    private int dataAccessorsCount;
    private boolean closed = false;

    public CallContext(CallContextBuilder contextBuilder) {
        CallContext context = contextBuilder.callContext;

        if (context != null) {
            this.celesta = context.celesta;
            this.userId = context.userId;
        } else {
            this.celesta = contextBuilder.celesta;
            this.userId = contextBuilder.userId;
        }

        this.conn = celesta.getConnectionPool().get();
        this.procName = contextBuilder.procName;
        this.dbPid = PIDSCACHE.computeIfAbsent(this.conn, getDbAdaptor()::getDBPid);
    }


    public Connection getConn() {
        return conn;
    }

    public String getUserId() {
        return userId;
    }


    /**
     * Коммитит транзакцию.
     */
    public void commit() {
        try {
            conn.commit();
        } catch (SQLException e) {
            throw new CelestaException("Commit unsuccessful: %s", e.getMessage());
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
        closed = true;
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
     * Возвращает время создания контекста вызова.
     */
    public Date getStartTime() {
        return startTime;
    }

    public boolean isClosed() {
        return closed;
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
            conn.close();
        } catch (Exception e) {
            throw new CelestaException("Can't close callContext", e);
        }
    }

    protected void rollback() throws SQLException {
        conn.rollback();
    }

    /**
     * Duplicates callcontext with another JDBC connection.
     */

    public CallContext getCopy() {
        //TODO!!
        return null;
    }


    public CallContextBuilder getBuilder() {
        return builder();
    }

    public static CallContextBuilder builder() {
        return new CallContextBuilder();
    }

    public static class CallContextBuilder {


        protected CallContext callContext = null;
        protected ICelesta celesta = null;
        protected String procName = null;
        protected String userId;


        protected CallContextBuilder getThis() {
            return this;
        }

        public CallContextBuilder setCallContext(CallContext callContext) {
            this.callContext = callContext;
            return getThis();
        }

        public CallContextBuilder setCelesta(ICelesta celesta) {
            this.celesta = celesta;
            return getThis();
        }

        public CallContextBuilder setProcName(String procName) {
            this.procName = procName;
            return getThis();
        }

        public CallContextBuilder setUserId(String userId) {
            this.userId = userId;
            return getThis();
        }

        public CallContext createCallContext() {
            return new CallContext(this);
        }
    }
}

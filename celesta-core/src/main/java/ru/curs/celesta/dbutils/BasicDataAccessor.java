package ru.curs.celesta.dbutils;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.score.GrainElement;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class BasicDataAccessor implements Closeable {
    static final String DATA_ACCESSOR_IS_CLOSED = "DataAccessor is closed.";


    private final CallContext context;

    private final Connection conn;
    private final DBAdaptor db;

    private BasicDataAccessor previousDataAccessor;
    private BasicDataAccessor nextDataAccessor;

    private boolean closed = false;


    public BasicDataAccessor(CallContext context) throws CelestaException {
        if (context == null)
            throw new CelestaException(
                    "Invalid context passed to %s constructor: context should not be null.",
                    this.getClass().getName());
        if (context.getConn() == null)
            throw new CelestaException(
                    "Invalid context passed to %s constructor: connection is null.",
                    this.getClass().getName());
        if (context.getUserId() == null)
            throw new CelestaException(
                    "Invalid context passed to %s constructor: user id is null.",
                    this.getClass().getName());
        if (context.isClosed())
            throw new CelestaException("Cannot create %s on a closed CallContext.",
                    this.getClass().getName());

        context.incDataAccessorsCount();
        this.context = context;

        previousDataAccessor = context.getLastDataAccessor();
        if (previousDataAccessor != null)
            previousDataAccessor.nextDataAccessor = this;
        context.setLastDataAccessor(this);

        this.conn = context.getConn();

        try {
            if (conn.isClosed())
                throw new CelestaException("Trying to create a cursor on closed connection.");
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage());
        }
        this.db = callContext().getDbAdaptor();
    }

    /**
     * Возвращает контекст вызова, в котором создан данный курсор.
     */
    public final CallContext callContext() {
        return context;
    }

    final DBAdaptor db() {
        return db;
    }

    final Connection conn() {
        return conn;
    }

    /**
     * Является ли объект доступа закрытым.
     */
    public boolean isClosed() {
        return closed;
    }

    public void close() {
        if (!isClosed()) {
            closed = true;
            if (this == callContext().getLastDataAccessor())
                context.setLastDataAccessor(previousDataAccessor);
            if (previousDataAccessor != null)
                previousDataAccessor.nextDataAccessor = nextDataAccessor;
            if (nextDataAccessor != null)
                nextDataAccessor.previousDataAccessor = previousDataAccessor;
            context.removeFromCache(this);
            context.decDataAccessorsCount();
        }
    }

    public abstract void clear() throws CelestaException;

    protected void clearSpecificState() {}

    /**
     * Объект метаданных (таблица, представление или последовательность), на основе которого создан
     * данный объект доступа.
     *
     * @throws CelestaException
     *             в случае ошибки извлечения метаинформации (в норме не должна
     *             происходить).
     */
    public abstract GrainElement meta() throws CelestaException;

    /**
     * Есть ли у сессии права на чтение текущего объекта.
     *
     * @throws CelestaException
     *             ошибка базы данных.
     */
    public final boolean canRead() throws CelestaException {
        if (isClosed())
            throw new CelestaException(DATA_ACCESSOR_IS_CLOSED);
        PermissionManager permissionManager = callContext().getPermissionManager();
        return permissionManager.isActionAllowed(callContext(), meta(), Action.READ);
    }


    // CHECKSTYLE:OFF
    	/*
	 * Эта группа методов именуется по правилам Python, а не Java. В Python
	 * имена protected-методов начинаются с underscore. Использование методов
	 * без underscore приводит к конфликтам с именами атрибутов.
	 */
    protected abstract String _grainName();
    //TODO:This is not correctly method name. Must be renamed to _objectName
    protected abstract String _tableName();
    // CHECKSTYLE:ON
}

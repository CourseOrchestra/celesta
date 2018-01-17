package ru.curs.celesta.dbutils;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ICallContext;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.score.GrainElement;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.SQLException;

public abstract class BasicDataAccessor extends CsqlBasicDataAccessor<CallContext> implements Closeable {

    private BasicDataAccessor previousDataAccessor;
    private BasicDataAccessor nextDataAccessor;

    public BasicDataAccessor(CallContext context) throws CelestaException {
        super(context);

        context.incDataAccessorsCount();

        previousDataAccessor = context.getLastDataAccessor();
        if (previousDataAccessor != null)
            previousDataAccessor.nextDataAccessor = this;
        context.setLastDataAccessor(this);
    }


    @Override
    protected void validateInitContext(CallContext context) throws CelestaException {
        super.validateInitContext(context);
        if (context.getUserId() == null)
            throw new CelestaException(
                    "Invalid context passed to %s constructor: user id is null.",
                    this.getClass().getName());
    }

    @Override
    void closeInternal() {
        if (this == callContext().getLastDataAccessor())
            callContext().setLastDataAccessor(previousDataAccessor);
        if (previousDataAccessor != null)
            previousDataAccessor.nextDataAccessor = nextDataAccessor;
        if (nextDataAccessor != null)
            nextDataAccessor.previousDataAccessor = previousDataAccessor;
        callContext().removeFromCache(this);
        callContext().decDataAccessorsCount();
    }

    protected void clearSpecificState() {}

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

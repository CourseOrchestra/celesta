package ru.curs.celesta.dbutils;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;

import java.io.Closeable;

public abstract class BasicDataAccessor extends CsqlBasicDataAccessor<CallContext> implements Closeable {

    private BasicDataAccessor previousDataAccessor;
    private BasicDataAccessor nextDataAccessor;

    public BasicDataAccessor(CallContext context) {
        super(context);

        context.incDataAccessorsCount();

        previousDataAccessor = context.getLastDataAccessor();
        if (previousDataAccessor != null)
            previousDataAccessor.nextDataAccessor = this;
        context.setLastDataAccessor(this);
    }


    @Override
    protected void validateInitContext(CallContext context) {
        super.validateInitContext(context);
        if (context.getUserId() == null)
            throw new CelestaException(
                    "Invalid context passed to %s constructor: user id is null.",
                    this.getClass().getName());
    }

    @Override
    protected void closeInternal() {
        if (this == callContext().getLastDataAccessor())
            callContext().setLastDataAccessor(previousDataAccessor);
        if (previousDataAccessor != null)
            previousDataAccessor.nextDataAccessor = nextDataAccessor;
        if (nextDataAccessor != null)
            nextDataAccessor.previousDataAccessor = previousDataAccessor;
        callContext().decDataAccessorsCount();
    }

    protected void clearSpecificState() {}

    /**
     * Есть ли у сессии права на чтение текущего объекта.
     */
    public final boolean canRead() {
        if (isClosed())
            throw new CelestaException(DATA_ACCESSOR_IS_CLOSED);
        IPermissionManager permissionManager = callContext().getPermissionManager();
        return permissionManager.isActionAllowed(callContext(), meta(), Action.READ);
    }


    // CHECKSTYLE:OFF
        /*
     * Эта группа методов именуется по правилам Python, а не Java. В Python
     * имена protected-методов начинаются с underscore. Использование методов
     * без underscore приводит к конфликтам с именами атрибутов.
     */
    protected abstract String _grainName();

    protected abstract String _objectName();
    // CHECKSTYLE:ON
}

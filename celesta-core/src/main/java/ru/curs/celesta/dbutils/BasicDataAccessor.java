package ru.curs.celesta.dbutils;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;

public abstract class BasicDataAccessor extends CsqlBasicDataAccessor<CallContext> {

    private BasicDataAccessor previousDataAccessor;
    private BasicDataAccessor nextDataAccessor;

    public BasicDataAccessor(CallContext context) {
        super(context);

        context.incDataAccessorsCount();

        previousDataAccessor = context.getLastDataAccessor();
        if (previousDataAccessor != null) {
            if (previousDataAccessor.nextDataAccessor != null) {
                throw new IllegalStateException();
            }
            previousDataAccessor.nextDataAccessor = this;
        }
        context.setLastDataAccessor(this);
    }


    @Override
    protected void validateInitContext(CallContext context) {
        super.validateInitContext(context);
        if (context.getUserId() == null) {
            throw new CelestaException(
                    "Invalid context passed to %s constructor: user id is null.",
                    this.getClass().getName());
        }
    }

    @Override
    protected void closeInternal() {
        if (this == callContext().getLastDataAccessor()) {
            callContext().setLastDataAccessor(previousDataAccessor);
        }
        if (previousDataAccessor != null) {
            previousDataAccessor.nextDataAccessor = nextDataAccessor;
        }
        if (nextDataAccessor != null) {
            nextDataAccessor.previousDataAccessor = previousDataAccessor;
        }
        //Help GC to avoid 'floating garbage'
        previousDataAccessor = null;
        nextDataAccessor = null;
        callContext().decDataAccessorsCount();
    }

    protected void clearSpecificState() {
    }

    /**
     * Whether the session has rights to read current object.
     *
     * @return
     */
    public final boolean canRead() {
        if (isClosed()) {
            throw new CelestaException(DATA_ACCESSOR_IS_CLOSED);
        }
        IPermissionManager permissionManager = callContext().getPermissionManager();
        return permissionManager.isActionAllowed(callContext(), meta(), Action.READ);
    }


    // CHECKSTYLE:OFF
    /*
     * This group of methods is named according to Python rules, and not Java.
     * In Python names of protected methods are started with an underscore symbol.
     * When using methods without an underscore symbol conflicts with attribute names
     * may be caused.
     */
    protected abstract String _grainName();

    protected abstract String _objectName();
    // CHECKSTYLE:ON
}

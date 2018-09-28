package ru.curs.celesta.dbutils;

import ru.curs.celesta.*;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.syscursors.CalllogCursor;

/**
 * Менеджер профилирования вызовов.
 */
public final class ProfilingManager implements IProfiler {

    private final Celesta celesta;
    private final DBAdaptor dbAdaptor;
    private boolean profilemode = false;


    public ProfilingManager(Celesta celesta, DBAdaptor dbAdaptor) {
        this.celesta = celesta;
        this.dbAdaptor = dbAdaptor;
    }

    /**
     * Записывает информацию о вызове в профилировщик.
     *
     * @param context контекст вызова.
     */
    public void logCall(CallContext context) {
        if (this.profilemode && !NO_LOG.equals(context.getProcName())) {
            try (CallContext sysContext = new SystemCallContext(celesta)) {
                CalllogCursor clc = new CalllogCursor(sysContext);
                clc.setProcname(context.getProcName());
                clc.setUserid(context.getUserId());

                clc.setStarttime(context.getStartTime());
                clc.setDuration((int) (context.getDurationNs() / 1000));
                clc.insert();
            }
        }
    }

    /**
     * Режим профилирования (записывается ли в таблицу calllog время вызовов
     * процедур).
     */
    public boolean isProfilemode() {
        return this.profilemode;
    }

    /**
     * Устанавливает режим профилирования.
     *
     * @param profilemode режим профилирования.
     */
    public void setProfilemode(boolean profilemode) {
        this.profilemode = profilemode;
    }
}

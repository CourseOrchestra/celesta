package ru.curs.celesta;

import ru.curs.celesta.dbutils.IProfiler;

import java.util.concurrent.ThreadLocalRandom;

public class SystemCallContext extends CallContext {

    //this is to avoid the temptation to hard-code a name of system user anywhere
    private final static String SYSUSER = String.format("SYS%08X",
            ThreadLocalRandom.current().nextInt());

    /**
     * Creates system call context. This context has permissions for everything.
     */
    public SystemCallContext() {
        super(SYSUSER);
    }

    /**
     * Creates and initializes system call context.
     *
     * @param celesta  Celesta to initialize the context with.
     * @param procName Proc name (for call logging).
     */
    public SystemCallContext(ICelesta celesta, String procName) {
        this();
        activate(celesta, procName);
    }

    /**
     * Creates and initializes system call context without call logging.
     *
     * @param celesta Celesta to initialize the context with.
     */
    public SystemCallContext(ICelesta celesta) {
        this();
        activate(celesta, IProfiler.NO_LOG);
    }
}

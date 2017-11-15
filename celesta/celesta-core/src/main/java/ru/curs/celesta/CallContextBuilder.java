package ru.curs.celesta;

import ru.curs.celesta.dbutils.LoggingManager;
import ru.curs.celesta.dbutils.PermissionManager;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.Score;

public class CallContextBuilder {

    private CallContext callContext = null;
    private Celesta celesta = null;
    private ConnectionPool connectionPool = null;
    private SessionContext sesContext = null;
    private Score score = null;
    private ShowcaseContext showcaseContext = null;
    private Grain curGrain = null;
    private String procName = null;
    private DBAdaptor dbAdaptor = null;
    private PermissionManager permissionManager;
    private LoggingManager loggingManager;

    public CallContextBuilder setCallContext(CallContext callContext) {
        this.callContext = callContext;
        return this;
    }

    public CallContextBuilder setCelesta(Celesta celesta) {
        this.celesta = celesta;
        return this;
    }

    public CallContextBuilder setConnectionPool(ConnectionPool connectionPool) {
        this.connectionPool = connectionPool;
        return this;
    }

    public CallContextBuilder setSesContext(SessionContext sesContext) {
        this.sesContext = sesContext;
        return this;
    }

    public CallContextBuilder setScore(Score score) {
        this.score = score;
        return this;
    }

    public CallContextBuilder setShowcaseContext(ShowcaseContext showcaseContext) {
        this.showcaseContext = showcaseContext;
        return this;
    }

    public CallContextBuilder setCurGrain(Grain curGrain) {
        this.curGrain = curGrain;
        return this;
    }

    public CallContextBuilder setProcName(String procName) {
        this.procName = procName;
        return this;
    }

    public CallContextBuilder setDbAdaptor(DBAdaptor dbAdaptor) {
        this.dbAdaptor = dbAdaptor;
        return this;
    }

    public CallContextBuilder setPermissionManager(PermissionManager permissionManager) {
        this.permissionManager = permissionManager;
        return this;
    }

    public CallContextBuilder setLoggingManager(LoggingManager loggingManager) {
        this.loggingManager = loggingManager;
        return this;
    }

    public CallContext createCallContext() throws CelestaException {
        return new CallContext(callContext, celesta, connectionPool, sesContext,
                showcaseContext, score, curGrain, procName, dbAdaptor, permissionManager, loggingManager);
    }
}
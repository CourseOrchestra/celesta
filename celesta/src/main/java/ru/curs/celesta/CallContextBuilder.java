package ru.curs.celesta;

import ru.curs.celesta.dbutils.ServiceManager;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.Score;

import java.util.HashMap;
import java.util.Map;

public class CallContextBuilder {
    private ConnectionPool connectionPool = null;
    private SessionContext sesContext = null;
    private Score score = null;
    private ShowcaseContext showcaseContext = null;
    private Grain curGrain = null;
    private String procName = null;
    private CallContext callContext = null;
    private DBAdaptor dbAdaptor = null;
    private Map<Class<? extends ServiceManager>, ? extends ServiceManager> serviceManagers = new HashMap<>();

    public CallContextBuilder setCallContext(CallContext callContext) {
        this.callContext = callContext;
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

    public CallContextBuilder setServiceManagers(Map<Class<? extends ServiceManager>, ? extends ServiceManager> serviceManagers) {
        this.serviceManagers = serviceManagers;
        return this;
    }

    public CallContext createCallContext() throws CelestaException {
        return new CallContext(callContext, connectionPool, sesContext,
                showcaseContext, score, curGrain, procName, dbAdaptor, serviceManagers);
    }
}
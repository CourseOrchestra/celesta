package ru.curs.celesta.java;

import ru.curs.celesta.SessionContext;

public class JSessionContext extends SessionContext {

    public JSessionContext(String userId, String sessionId) {
        super(userId, sessionId);
    }

    @Override
    protected JCallContext.JCallContextBuilder callContextBuilder() {
        return JCallContext.builder();
    }
}

package ru.curs.celesta;

public class JSessionContext extends SessionContext {

    public JSessionContext(String userId, String sessionId) {
        super(userId, sessionId);
    }

    @Override
    JCallContext.JCallContextBuilder callContextBuilder() {
        return JCallContext.builder();
    }
}

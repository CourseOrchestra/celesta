package ru.curs.celesta;

import java.util.Random;

public abstract class SessionContext {

    public static final String SYSTEM_USER_ID = String.format("SYS%08X", (new Random()).nextInt());
    public static final String SYSTEM_SESSION_ID = "CELESTA";

    private final String userId;
    private final String sessionId;

    public SessionContext(String userId, String sessionId) {
        this.userId = userId;
        this.sessionId = sessionId;
    }

    /**
     * Имя пользователя.
     */
    public String getUserId() {
        return userId;
    }

    /**
     * Идентификатор сессии пользователя.
     */
    public String getSessionId() {
        return sessionId;
    }

    abstract protected CallContext.CallContextBuilder callContextBuilder();
}

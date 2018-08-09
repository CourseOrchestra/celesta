package ru.curs.celesta;

import java.util.concurrent.ThreadLocalRandom;

public abstract class SessionContext {
    /**
     * User id for system tasks, like DB upgrading. Generated randomly each time to prevent its usage by
     * business logic developers.
     */
    public static final String SYSTEM_USER_ID = String.format("SYS%08X", ThreadLocalRandom.current().nextInt());
    /**
     * Session id for system tasks.
     */
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

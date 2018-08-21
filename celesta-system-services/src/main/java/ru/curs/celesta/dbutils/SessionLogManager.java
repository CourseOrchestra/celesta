package ru.curs.celesta.dbutils;

import java.util.Date;

import ru.curs.celesta.*;
import ru.curs.celesta.syscursors.SessionlogCursor;

/**
 * Менеджер записи в лог сведений о входах и выходах пользователей.
 */
public final class SessionLogManager {

    private final AbstractCelesta celesta;
    private final boolean enabled;

    public SessionLogManager(AbstractCelesta celesta, boolean enabled) {
        this.celesta = celesta;
        this.enabled = enabled;
    }

    /**
     * Записывает данные о входе сессии.
     *
     * @param session Сессия.
     */
    public void logLogin(SessionContext session) {
        if (!enabled) return;

        try (CallContext context = celesta.callContext()) {
            SessionlogCursor sl = new SessionlogCursor(context);
            sl.init();
            sl.setSessionid(session.getSessionId());
            sl.setUserid(session.getUserId());
            sl.insert();
        }
    }

    /**
     * Записывает информацию о неудачном логине.
     *
     * @param userId Имя пользователя.
     */
    public void logFailedLogin(String userId) {
        if (!enabled) return;

        try (CallContext context = celesta.callContext()) {
            SessionlogCursor sl = new SessionlogCursor(context);
            sl.init();
            sl.setUserid(userId);
            sl.setFailedlogin(true);
            sl.insert();
        }
    }

    /**
     * Записывает данные в лог о выходе из сессии.
     *
     * @param session Сессия
     * @param timeout Выход по таймауту.
     */
    public void logLogout(SessionContext session, boolean timeout) {
        if (!enabled) return;

        try (CallContext context = celesta.callContext()) {
            SessionlogCursor sl = new SessionlogCursor(context);
            sl.init();
            sl.setRange("sessionid", session.getSessionId());
            sl.setRange("userid", session.getUserId());
            sl.orderBy("entryno DESC");
            if (sl.tryFirst()) {
                sl.setLogoutime(new Date());
                sl.setTimeout(timeout);
                sl.update();
            }
        }
    }
}

package ru.curs.celesta.dbutils;

import java.sql.Connection;
import java.util.Date;

import ru.curs.celesta.*;
import ru.curs.celesta.syscursors.SessionLogCursor;

/**
 * Менеджер записи в лог сведений о входах и выходах пользователей.
 */
public final class SessionLogManager {

	private final Celesta celesta;
	private final boolean enabled;

	public SessionLogManager(Celesta celesta, boolean enabled) {
		this.celesta = celesta;
		this.enabled = enabled;
	}

	/**
	 * Записывает данные о входе сессии.
	 * 
	 * @param session
	 *            Сессия.
	 * @throws CelestaException
	 *             Ошибка взаимодействия с БД.
	 */
	public void logLogin(SessionContext session) throws CelestaException {
			if (!enabled) return;

			try (CallContext context = celesta.callContext(BasicCursor.SYSTEMSESSION)) {
				SessionLogCursor sl = new SessionLogCursor(context);
				sl.init();
				sl.setSessionid(session.getSessionId());
				sl.setUserid(session.getUserId());
				sl.insert();
			}
	}

	/**
	 * Записывает информацию о неудачном логине.
	 * 
	 * @param userId
	 *            Имя пользователя.
	 * @throws CelestaException
	 *             Если не удалось связаться с БД.
	 */
	public void logFailedLogin(String userId) throws CelestaException {
			if (!enabled) return;

			try (CallContext context = celesta.callContext(BasicCursor.SYSTEMSESSION)) {
				SessionLogCursor sl = new SessionLogCursor(context);
				sl.init();
				sl.setUserid(userId);
				sl.setFailedlogin(true);
				sl.insert();
			}
	}

	/**
	 * Записывает данные в лог о выходе из сессии.
	 * 
	 * @param session
	 *            Сессия
	 * @param timeout
	 *            Выход по таймауту.
	 * @throws CelestaException
	 *             Ошибка взаимодействия с БД.
	 */
	public void logLogout(SessionContext session, boolean timeout)
			throws CelestaException {
			if (!enabled) return;

			try (CallContext context = celesta.callContext(BasicCursor.SYSTEMSESSION)) {
				SessionLogCursor sl = new SessionLogCursor(context);
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

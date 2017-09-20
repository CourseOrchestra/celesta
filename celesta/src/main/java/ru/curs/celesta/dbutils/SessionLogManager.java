package ru.curs.celesta.dbutils;

import java.sql.Connection;
import java.util.Date;

import ru.curs.celesta.AppSettings;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.SessionContext;
import ru.curs.celesta.syscursors.SessionLogCursor;

/**
 * Менеджер записи в лог сведений о входах и выходах пользователей.
 */
public final class SessionLogManager {

	private final boolean enabled;

	public SessionLogManager(boolean enabled) {
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

			Connection conn = ConnectionPool.get();
			CallContext context = new CallContext(conn,
					BasicCursor.SYSTEMSESSION);
			try {
				SessionLogCursor sl = new SessionLogCursor(context);
				sl.init();
				sl.setSessionid(session.getSessionId());
				sl.setUserid(session.getUserId());
				sl.insert();
			} finally {
				context.closeCursors();
				ConnectionPool.putBack(conn);
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

			Connection conn = ConnectionPool.get();
			CallContext context = new CallContext(conn,
					BasicCursor.SYSTEMSESSION);
			try {
				SessionLogCursor sl = new SessionLogCursor(context);
				sl.init();
				sl.setUserid(userId);
				sl.setFailedlogin(true);
				sl.insert();
			} finally {
				context.closeCursors();
				ConnectionPool.putBack(conn);
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

			Connection conn = ConnectionPool.get();
			CallContext context = new CallContext(conn,
					BasicCursor.SYSTEMSESSION);
			try {
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
			} finally {
				context.closeCursors();
				ConnectionPool.putBack(conn);
			}
	}
}

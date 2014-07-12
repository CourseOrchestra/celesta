package ru.curs.celesta.dbutils;

import java.sql.Connection;
import java.util.Date;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.SessionContext;
import ru.curs.celesta.syscursors.SessionLogCursor;

/**
 * Менеджер записи в лог сведений о входах и выходах пользователей.
 */
public final class SessionLogManager {
	private SessionLogManager() {

	}

	/**
	 * Записывает данные о входе сессии.
	 * 
	 * @param session
	 *            Сессия.
	 * @throws CelestaException
	 *             Ошибка взаимодействия с БД.
	 */
	public static void logLogin(SessionContext session) throws CelestaException {
		Connection conn = ConnectionPool.get();
		CallContext context = new CallContext(conn, BasicCursor.SYSTEMSESSION);
		try {
			SessionLogCursor sl = new SessionLogCursor(context);
			sl.init();
			sl.setSessionid(session.getSessionId());
			sl.setUserid(session.getUserId());
			sl.insert();
		} finally {
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
	public static void logLogout(SessionContext session, boolean timeout)
			throws CelestaException {
		Connection conn = ConnectionPool.get();
		CallContext context = new CallContext(conn, BasicCursor.SYSTEMSESSION);
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
			ConnectionPool.putBack(conn);
		}
	}
}

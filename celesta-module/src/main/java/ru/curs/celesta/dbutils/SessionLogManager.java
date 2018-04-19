package ru.curs.celesta.dbutils;

import java.util.Date;

import ru.curs.celesta.*;
import ru.curs.celesta.syscursors.SessionlogCursor;

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
	public void logLogin(PySessionContext session) {
			if (!enabled) return;

			try (CallContext context = celesta.callContext(PySessionContext.SYSTEMSESSION)) {
				SessionlogCursor sl = new SessionlogCursor(context);
				sl.init();
				sl.setSessionid(session.getSessionId());
				sl.setUserid(session.getUserId());
				sl.insert();
			} catch (CelestaException e) {
				throw new RuntimeException(e);
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
	public void logFailedLogin(String userId) {
			if (!enabled) return;

			try (CallContext context = celesta.callContext(PySessionContext.SYSTEMSESSION)) {
				SessionlogCursor sl = new SessionlogCursor(context);
				sl.init();
				sl.setUserid(userId);
				sl.setFailedlogin(true);
				sl.insert();
			} catch (CelestaException e) {
				throw new RuntimeException(e);
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
	public void logLogout(PySessionContext session, boolean timeout) {
			if (!enabled) return;

			try (CallContext context = celesta.callContext(PySessionContext.SYSTEMSESSION)) {
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
			} catch (CelestaException e) {
				throw new RuntimeException(e);
			}
	}
}

package ru.curs.celesta.dbutils;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CallContextBuilder;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.syscursors.CallLogCursor;

/**
 * Менеджер профилирования вызовов.
 *
 */
public final class ProfilingManager {

	private final DBAdaptor dbAdaptor;
	private boolean profilemode = false;


	public ProfilingManager(DBAdaptor dbAdaptor) {
		this.dbAdaptor = dbAdaptor;
	}

	/**
	 * Записывает информацию о вызове в профилировщик.
	 * 
	 * @param context
	 *            контекст вызова.
	 * @throws CelestaException
	 *             Если запись невозможна.
	 */
	public void logCall(CallContext context) throws CelestaException {
		if (profilemode) {
			long finish = System.currentTimeMillis();

			try (
					CallContext sysContext = new CallContextBuilder()
							.setCallContext(context)
							.setSesContext(BasicCursor.SYSTEMSESSION)
							.setDbAdaptor(dbAdaptor)
							.createCallContext()
			) {
				CallLogCursor clc = new CallLogCursor(sysContext);
				clc.setProcname(context.getProcName());
				clc.setSessionid(context.getSessionId());
				clc.setUserid(context.getUserId());
				clc.setStarttime(context.getStartTime());
				clc.setDuration((int) (finish - context.getStartTime().getTime()));
				clc.insert();
			}
		}
	}

	/**
	 * Режим профилирования (записывается ли в таблицу calllog время вызовов
	 * процедур).
	 */
	public boolean isProfilemode() {
		return profilemode;
	}

	/**
	 * Устанавливает режим профилирования.
	 * 
	 * @param profilemode
	 *            режим профилирования.
	 */
	public void setProfilemode(boolean profilemode) {
		this.profilemode = profilemode;
	}
}

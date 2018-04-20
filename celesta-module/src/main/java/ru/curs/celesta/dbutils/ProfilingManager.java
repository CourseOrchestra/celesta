package ru.curs.celesta.dbutils;

import ru.curs.celesta.*;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.syscursors.CalllogCursor;

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
	 */
	public void logCall(CallContext context) {
		if (profilemode) {
			long finish = System.currentTimeMillis();

			try (
					CallContext sysContext = PyCallContext.builder()
							.setCallContext((PyCallContext)context)
							.setSesContext(PySessionContext.SYSTEMSESSION)
							.setDbAdaptor(dbAdaptor)
							.createCallContext()
			) {
				CalllogCursor clc = new CalllogCursor(sysContext);
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

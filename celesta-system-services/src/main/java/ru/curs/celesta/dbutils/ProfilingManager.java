package ru.curs.celesta.dbutils;

import ru.curs.celesta.*;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.syscursors.CalllogCursor;

/**
 * Менеджер профилирования вызовов.
 *
 */
public final class ProfilingManager {

	private final AbstractCelesta celesta;
	private final DBAdaptor dbAdaptor;
	private boolean profilemode = false;


	public ProfilingManager(AbstractCelesta celesta, DBAdaptor dbAdaptor) {
		this.celesta = celesta;
		this.dbAdaptor = dbAdaptor;
	}

	/**
	 * Записывает информацию о вызове в профилировщик.
	 * 
	 * @param context
	 *            контекст вызова.
	 */
	public void logCall(CallContext context) {
		if (this.profilemode) {
			long finish = System.currentTimeMillis();

			try (
					CallContext sysContext = context.getBuilder()
							.setCallContext(context)
							.setSesContext(this.celesta.getSystemSessionContext())
							.setDbAdaptor(this.dbAdaptor)
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
		return this.profilemode;
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

package ru.curs.celesta.dbutils;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.Table;
import ru.curs.celesta.syscursors.LogCursor;
import ru.curs.celesta.syscursors.LogSetupCursor;

/**
 * Менеджер логирования. Записывает в лог изменённые значения (если это
 * необходимо).
 * 
 */
final class LoggingManager {
	/**
	 * Размер кэша (в записях). ДОЛЖЕН БЫТЬ СТЕПЕНЬЮ ДВОЙКИ!! Этот кэш может
	 * быть меньше кэша системы распределения прав доступа, т. к. хранит записи
	 * на уровне таблицы, а не на уровне комбинации "пользователь, таблица".
	 */
	private static final int CACHE_SIZE = 1024;
	/**
	 * "Срок годности" записи кэша (в миллисекундах).
	 */
	private static final int CACHE_ENTRY_SHELF_LIFE = 20000;

	private CacheEntry[] cache = new CacheEntry[CACHE_SIZE];

	/**
	 * Запись во внутреннем кэше.
	 */
	private static class CacheEntry {
		private final Table table;
		private final int loggingMask;
		private final long expirationTime;

		CacheEntry(Table table, int loggingMask) {
			this.table = table;
			this.loggingMask = loggingMask;
			expirationTime = System.currentTimeMillis()
					+ CACHE_ENTRY_SHELF_LIFE;

		}

		public static int hash(Table table) {
			return (table.getGrain().getName() + '|' + table.getName())
					.hashCode();
		}

		public boolean isExpired() {
			return System.currentTimeMillis() > expirationTime;
		}

		public boolean isLoggingNeeded(Action a) {
			if (a == Action.READ)
				throw new IllegalArgumentException();
			return (loggingMask & a.getMask()) != 0;
		}

	}

	boolean isLoggingNeeded(CallContext sysContext, Table t, Action a)
			throws CelestaException {
		// Вычисляем местоположение данных в кэше.
		int index = CacheEntry.hash(t) & (CACHE_SIZE - 1);

		// Прежде всего смотрим, нет ли в кэше подходящей непросроченной записи
		// (в противном случае -- обновляем кэш).
		CacheEntry ce = cache[index];
		if (ce == null || ce.isExpired() || ce.table != t) {
			ce = refreshLogging(sysContext, t);
			cache[index] = ce;
		}
		return ce.isLoggingNeeded(a);
	}

	private CacheEntry refreshLogging(CallContext sysContext, Table t)
			throws CelestaException {
		LogSetupCursor logsetup = new LogSetupCursor(sysContext);
		int loggingMask = 0;
		if (logsetup.tryGet(t.getGrain().getName(), t.getName())) {
			loggingMask |= logsetup.isI() ? Action.INSERT.getMask() : 0;
			loggingMask |= logsetup.isM() ? Action.MODIFY.getMask() : 0;
			loggingMask |= logsetup.isD() ? Action.DELETE.getMask() : 0;
		}
		return new CacheEntry(t, loggingMask);
	}

	public void log(Cursor c, Action a) throws CelestaException {
		if (a == Action.READ)
			throw new IllegalArgumentException();
		CallContext sysContext = new CallContext(c.callContext().getConn(),
				BasicCursor.SYSTEMUSERID, null);
		if (!isLoggingNeeded(sysContext, c.meta(), a))
			return;
		LogCursor log = new LogCursor(sysContext);
		log.init();
		log.setUserid(c.callContext().getUserId());
		log.setGrainid(c._grainName());
		log.setTablename(c._tableName());
		log.setActionType(a.shortId());
		Object[] o = c._currentKeyValues();

		String value;
		int len;
		if (o.length > 0) {
			value = o[0] == null ? "NULL" : o[0].toString();
			len = log.getMaxStrLen("pkvalue1");
			log.setPkvalue1(trimValue(value, len));
		}
		if (o.length > 1) {
			value = o[1] == null ? "NULL" : o[1].toString();
			len = log.getMaxStrLen("pkvalue2");
			log.setPkvalue2(trimValue(value, len));
		}
		if (o.length > 2) {
			value = o[2] == null ? "NULL" : o[2].toString();
			len = log.getMaxStrLen("pkvalue3");
			log.setPkvalue3(trimValue(value, len));
		}

		len = log.getMaxStrLen("newvalues");
		switch (a) {
		case INSERT:
			value = c.asCSVLine();
			log.setNewvalues(trimValue(value, len));
			break;
		case MODIFY:
			value = c.asCSVLine();
			log.setNewvalues(trimValue(value, len));

			value = c.getXRec().asCSVLine();
			log.setOldvalues(trimValue(value, len));
			break;
		case DELETE:
			value = c.getXRec().asCSVLine();
			log.setOldvalues(trimValue(value, len));
			break;
		default:
		}
		log.insert();
	}

	private static String trimValue(String value, int len) {
		return value.length() > len ? value.substring(0, len) : value;
	}
}

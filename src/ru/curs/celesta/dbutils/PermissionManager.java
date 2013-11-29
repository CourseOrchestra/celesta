package ru.curs.celesta.dbutils;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.Table;
import ru.curs.celesta.syscursors.PermissionsCursor;
import ru.curs.celesta.syscursors.UserRolesCursor;

/**
 * Менеджер пермиссий. Определяет, имеет ли право тот или иной пользователь на
 * операции с таблицей. Права определяются по содержимому системных таблиц
 * распределения прав доступа.
 * 
 * Для оптимизации работы объект содержит кэш.
 * 
 */
final class PermissionManager {

	/**
	 * Имя роли, обладающей правами на чтение всех таблиц.
	 */
	public static final String EDITOR = "editor";
	/**
	 * Имя роли, обладающей правами на редактирование всех таблиц.
	 */
	public static final String READER = "reader";
	/**
	 * Размер кэша (в записях). ДОЛЖЕН БЫТЬ СТЕПЕНЬЮ ДВОЙКИ!!
	 */
	private static final int CACHE_SIZE = 2048;
	/**
	 * "Срок годности" записи кэша (в миллисекундах).
	 */
	private static final int CACHE_ENTRY_SHELF_LIFE = 10000;

	private static final int FULL_RIGHTS = Action.READ.getMask()
			| Action.INSERT.getMask() | Action.MODIFY.getMask()
			| Action.DELETE.getMask();

	private CacheEntry[] cache = new CacheEntry[CACHE_SIZE];

	/**
	 * Запись во внутреннем кэше.
	 * 
	 */
	private static class CacheEntry {

		private final String userName;
		private final Table table;

		private final int permissionMask;

		private final long expirationTime;

		public CacheEntry(String userName, Table table, int permissionMask) {
			if (userName == null)
				throw new IllegalArgumentException();
			this.userName = userName;
			this.table = table;
			this.permissionMask = permissionMask;

			expirationTime = System.currentTimeMillis()
					+ CACHE_ENTRY_SHELF_LIFE;
		}

		public static int hash(String userName, Table table) {
			return (userName + '|' + table.getGrain().getName() + '|' + table
					.getName()).hashCode();
		}

		public boolean isExpired() {
			return System.currentTimeMillis() > expirationTime;
		}

		public boolean isActionPermitted(Action a) {
			return (permissionMask & a.getMask()) != 0;
		}

	}

	/**
	 * Разрешено ли действие.
	 * 
	 * @param c
	 *            контекст вызова.
	 * @param t
	 *            таблица.
	 * 
	 * @param a
	 *            тип действия
	 * @throws CelestaException
	 *             ошибка БД
	 */
	public boolean isActionAllowed(CallContext c, Table t, Action a)
			throws CelestaException {
		// Системному пользователю дозволяется всё без дальнейшего
		// разбирательства.
		if (Cursor.SYSTEMUSERID.equals(c.getUserId()))
			return true;

		// Вычисляем местоположение данных в кэше.
		int index = CacheEntry.hash(c.getUserId(), t) & (CACHE_SIZE - 1);

		// Прежде всего смотрим, нет ли в кэше подходящей непросроченной записи
		// (в противном случае -- обновляем кэш).
		CacheEntry ce = cache[index];
		if (ce == null || ce.isExpired() || ce.table != t
				|| !ce.userName.equals(c.getUserId())) {
			ce = refreshPermissions(c, t);
			cache[index] = ce;
		}
		return ce.isActionPermitted(a);
	}

	private CacheEntry refreshPermissions(CallContext c, Table t)
			throws CelestaException {
		CallContext sysContext = new CallContext(c.getConn(),
				Cursor.SYSTEMUSERID, null);
		UserRolesCursor userRoles = new UserRolesCursor(sysContext);
		PermissionsCursor permissions = new PermissionsCursor(sysContext);
		userRoles.setRange("userid", c.getUserId());

		int permissionsMask = 0;

		while (userRoles.next() && permissionsMask != FULL_RIGHTS) {
			if (READER.equals(userRoles.getRoleId())
					|| (t.getGrain().getName() + '.' + READER).equals(userRoles
							.getRoleId())) {
				permissionsMask |= Action.READ.getMask();
			} else if (EDITOR.equals(userRoles.getRoleId())
					|| (t.getGrain().getName() + '.' + EDITOR).equals(userRoles
							.getRoleId())) {
				permissionsMask = FULL_RIGHTS;
			} else if (permissions.tryGet(userRoles.getRoleId(), t.getGrain()
					.getName(), t.getName())) {
				permissionsMask |= permissions.isR() ? Action.READ.getMask()
						: 0;
				permissionsMask |= permissions.isI() ? Action.INSERT.getMask()
						: 0;
				permissionsMask |= permissions.isM() ? Action.MODIFY.getMask()
						: 0;
				permissionsMask |= permissions.isD() ? Action.DELETE.getMask()
						: 0;
			}
		}
		return new CacheEntry(c.getUserId(), t, permissionsMask);
	}
}

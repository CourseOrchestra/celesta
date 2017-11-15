package ru.curs.celesta.dbutils;

import java.util.ArrayList;
import java.util.List;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CallContextBuilder;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.score.GrainElement;
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
public final class PermissionManager {

	/**
	 * Имя роли, обладающей правами на редактирование всех таблиц.
	 */
	public static final String EDITOR = "editor";
	/**
	 * Имя роли, обладающей правами на чтение всех таблиц.
	 */
	public static final String READER = "reader";
	/**
	 * Размер кэша (в записях). ДОЛЖЕН БЫТЬ СТЕПЕНЬЮ ДВОЙКИ!!
	 */
	private static final int CACHE_SIZE = 8192;

	private static final int ROLE_CACHE_SIZE = 2048;
	/**
	 * "Срок годности" записи кэша (в миллисекундах).
	 */
	private static final int CACHE_ENTRY_SHELF_LIFE = 20000;

	private static final int FULL_RIGHTS = Action.READ.getMask()
			| Action.INSERT.getMask() | Action.MODIFY.getMask()
			| Action.DELETE.getMask();

	private final DBAdaptor dbAdaptor;
	private PermissionCacheEntry[] cache = new PermissionCacheEntry[CACHE_SIZE];
	private RoleCacheEntry[] rolesCache = new RoleCacheEntry[ROLE_CACHE_SIZE];

	/**
	 * Базовый класс элемента кэша менеджера пермиссий.
	 * 
	 */
	private static class BaseCacheEntry {
		private final long expirationTime;

		BaseCacheEntry() {
			expirationTime = System.currentTimeMillis()
					+ CACHE_ENTRY_SHELF_LIFE;
		}

		public boolean isExpired() {
			return System.currentTimeMillis() > expirationTime;
		}
	}

	/**
	 * Запись во внутреннем кэше.
	 * 
	 */
	private static class PermissionCacheEntry extends BaseCacheEntry {
		private final String userName;
		private final GrainElement table;
		private final int permissionMask;

		public PermissionCacheEntry(String userName, GrainElement table,
				int permissionMask) {
			super();
			if (userName == null)
				throw new IllegalArgumentException();
			this.userName = userName;
			this.table = table;
			this.permissionMask = permissionMask;
		}

		public static int hash(String userName, GrainElement table) {
			return (userName + '|' + table.getGrain().getName() + '|' + table
					.getName()).hashCode();
		}

		public boolean isActionPermitted(Action a) {
			return (permissionMask & a.getMask()) != 0;
		}

	}

	/**
	 * Запись в кэше ролей пользователя.
	 */
	private static class RoleCacheEntry extends BaseCacheEntry {
		private final String userId;
		private final List<String> roles = new ArrayList<>();

		RoleCacheEntry(String userId) {
			super();
			this.userId = userId;
		}
	}


	public PermissionManager(DBAdaptor dbAdaptor) {
		this.dbAdaptor = dbAdaptor;
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
	public boolean isActionAllowed(CallContext c, GrainElement t, Action a)
			throws CelestaException {
		// Системному пользователю дозволяется всё без дальнейшего
		// разбирательства.
		if (BasicCursor.SYSTEMUSERID.equals(c.getUserId()))
			return true;

		// Вычисляем местоположение данных в кэше.
		int index = PermissionCacheEntry.hash(c.getUserId(), t)
				& (CACHE_SIZE - 1);

		// Прежде всего смотрим, нет ли в кэше подходящей непросроченной записи
		// (в противном случае -- обновляем кэш).
		PermissionCacheEntry ce = cache[index];
		if (ce == null || ce.isExpired() || ce.table != t
				|| !ce.userName.equals(c.getUserId())) {
			ce = refreshPermissions(c, t);
			cache[index] = ce;
		}
		return ce.isActionPermitted(a);
	}

	private RoleCacheEntry getRce(String userID, CallContext sysContext)
			throws CelestaException {
		int index = userID.hashCode() & (ROLE_CACHE_SIZE - 1);
		RoleCacheEntry rce = rolesCache[index];
		if (rce == null || rce.isExpired() || !rce.userId.equals(userID)) {
			rce = new RoleCacheEntry(userID);
			UserRolesCursor userRoles = new UserRolesCursor(sysContext);
			userRoles.setRange("userid", userID);
			while (userRoles.nextInSet()) {
				rce.roles.add(userRoles.getRoleid());
			}
			rolesCache[index] = rce;
		}
		return rce;
	}

	private PermissionCacheEntry refreshPermissions(CallContext c,
			GrainElement t) throws CelestaException {
		try (
				CallContext sysContext = new CallContextBuilder()
						.setCallContext(c)
						.setSesContext(BasicCursor.SYSTEMSESSION)
						.setDbAdaptor(dbAdaptor)
						.createCallContext()
		) {
			RoleCacheEntry rce = getRce(c.getUserId(), sysContext);
			PermissionsCursor permissions = new PermissionsCursor(sysContext);
			int permissionsMask = 0;
			for (String roleId : rce.roles) {
				if (permissionsMask == FULL_RIGHTS)
					break;
				if (READER.equals(roleId)
						|| (t.getGrain().getName() + '.' + READER)
								.equals(roleId)) {
					permissionsMask |= Action.READ.getMask();
				} else if (EDITOR.equals(roleId)
						|| (t.getGrain().getName() + '.' + EDITOR)
								.equals(roleId)) {
					permissionsMask = FULL_RIGHTS;
				} else if (permissions.tryGet(roleId, t.getGrain().getName(),
						t.getName())) {
					permissionsMask |= permissions.isR() ? Action.READ
							.getMask() : 0;
					permissionsMask |= permissions.isI() ? Action.INSERT
							.getMask() : 0;
					permissionsMask |= permissions.isM() ? Action.MODIFY
							.getMask() : 0;
					permissionsMask |= permissions.isD() ? Action.DELETE
							.getMask() : 0;
				}
			}
			return new PermissionCacheEntry(c.getUserId(), t, permissionsMask);
		}

	}
}

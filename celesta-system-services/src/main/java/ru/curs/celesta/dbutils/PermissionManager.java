package ru.curs.celesta.dbutils;

import java.util.ArrayList;
import java.util.List;

import ru.curs.celesta.*;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.score.GrainElement;
import ru.curs.celesta.syscursors.PermissionsCursor;
import ru.curs.celesta.syscursors.UserrolesCursor;

/**
 * Менеджер пермиссий. Определяет, имеет ли право тот или иной пользователь на
 * операции с таблицей. Права определяются по содержимому системных таблиц
 * распределения прав доступа.
 * <p>
 * Для оптимизации работы объект содержит кэш.
 */
public final class PermissionManager implements IPermissionManager {
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

    private final ICelesta celesta;
    private final DBAdaptor dbAdaptor;
    private PermissionCacheEntry[] cache = new PermissionCacheEntry[CACHE_SIZE];
    private RoleCacheEntry[] rolesCache = new RoleCacheEntry[ROLE_CACHE_SIZE];

    /**
     * Базовый класс элемента кэша менеджера пермиссий.
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


    public PermissionManager(ICelesta celesta, DBAdaptor dbAdaptor) {
        this.celesta = celesta;
        this.dbAdaptor = dbAdaptor;
    }

    public boolean isActionAllowed(CallContext c, GrainElement t, Action a) {
        // Системному пользователю дозволяется всё без дальнейшего
        // разбирательства.
        if (c instanceof SystemCallContext)
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

    private RoleCacheEntry getRce(String userID, CallContext sysContext) {
        int index = userID.hashCode() & (ROLE_CACHE_SIZE - 1);
        RoleCacheEntry rce = rolesCache[index];
        if (rce == null || rce.isExpired() || !rce.userId.equals(userID)) {
            rce = new RoleCacheEntry(userID);
            UserrolesCursor userRoles = new UserrolesCursor(sysContext);
            userRoles.setRange("userid", userID);
            while (userRoles.nextInSet()) {
                rce.roles.add(userRoles.getRoleid());
            }
            rolesCache[index] = rce;
        }
        return rce;
    }

    private PermissionCacheEntry refreshPermissions(CallContext c, GrainElement t) {
        try (CallContext sysContext = new SystemCallContext(celesta, "refreshPermissions")) {
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
                    permissionsMask |= permissions.getR() ? Action.READ
                            .getMask() : 0;
                    permissionsMask |= permissions.getI() ? Action.INSERT
                            .getMask() : 0;
                    permissionsMask |= permissions.getM() ? Action.MODIFY
                            .getMask() : 0;
                    permissionsMask |= permissions.getD() ? Action.DELETE
                            .getMask() : 0;
                }
            }
            return new PermissionCacheEntry(c.getUserId(), t, permissionsMask);
        }

    }
}

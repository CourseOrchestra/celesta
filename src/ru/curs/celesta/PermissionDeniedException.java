package ru.curs.celesta;

import ru.curs.celesta.dbutils.Action;
import ru.curs.celesta.score.Table;

/**
 * Исключение, возникающее при отсутствии разрешений на таблицу.
 * 
 */
public class PermissionDeniedException extends CelestaException {

	private static final long serialVersionUID = 1L;

	public PermissionDeniedException(String message) {
		super(message);
	}

	public PermissionDeniedException(String message, Object... args) {
		super(message, args);
	}

	public PermissionDeniedException(CallContext context, Table table,
			Action action) {
		this("There is no %s permission for user %s on table %s.%s", action
				.toString(), context.getUserId(), table.getGrain().getName(),
				table.getName());
	}

}

package ru.curs.celesta.syscursors;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.Cursor;

/**
 * Базовый класс курсора системной таблицы (относящейся к грануле "celesta").
 * Эти классы объединяет общий результат метода grainName(), а также пустая
 * реализация методов-триггеров.
 */
public abstract class SysCursor extends Cursor {

	public SysCursor(CallContext context) throws CelestaException {
		super(context);
	}

	@Override
	// CHECKSTYLE:OFF
	protected final String _grainName() {
		return "celesta";
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _preDelete() {
		// CHECKSTYLE:ON
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _postDelete() {
		// CHECKSTYLE:ON
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _preUpdate() {
		// CHECKSTYLE:ON
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _postUpdate() {
		// CHECKSTYLE:ON
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _preInsert() {
		// CHECKSTYLE:ON
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _postInsert() {
		// CHECKSTYLE:ON
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _setAutoIncrement(int val) {
		// CHECKSTYLE:ON
		// do nothing by default
	}

}
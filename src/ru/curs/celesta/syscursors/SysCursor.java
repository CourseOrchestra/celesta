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
	protected final String grainName() {
		return "celesta";
	}

	@Override
	protected void preDelete() {
	}

	@Override
	protected void postDelete() {
	}

	@Override
	protected void preUpdate() {
	}

	@Override
	protected void postUpdate() {
	}

	@Override
	protected void preInsert() {
	}

	@Override
	protected void postInsert() {
	}

	@Override
	protected void setAutoIncrement(int val) {
		// do nothing by default
	}

}
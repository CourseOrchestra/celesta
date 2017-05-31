package ru.curs.celesta.syscursors;

import java.lang.reflect.Field;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.Celesta;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.Cursor;
import ru.curs.celesta.event.TriggerType;

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
		try {
			Celesta.getInstance().getTriggerDispatcher().fireTrigger(TriggerType.PRE_DELETE, this);
		} catch (CelestaException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _postDelete() {
		// CHECKSTYLE:ON
		try {
			Celesta.getInstance().getTriggerDispatcher().fireTrigger(TriggerType.POST_DELETE, this);
		} catch (CelestaException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _preUpdate() {
		// CHECKSTYLE:ON
		try {
			Celesta.getInstance().getTriggerDispatcher().fireTrigger(TriggerType.PRE_UPDATE, this);
		} catch (CelestaException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _postUpdate() {
		// CHECKSTYLE:ON
		try {
			Celesta.getInstance().getTriggerDispatcher().fireTrigger(TriggerType.POST_UPDATE, this);
		} catch (CelestaException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _preInsert() {
		// CHECKSTYLE:ON
		try {
			Celesta.getInstance().getTriggerDispatcher().fireTrigger(TriggerType.PRE_INSERT, this);
		} catch (CelestaException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _postInsert() {
		// CHECKSTYLE:ON
		try {
			Celesta.getInstance().getTriggerDispatcher().fireTrigger(TriggerType.POST_INSERT, this);
		} catch (CelestaException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _setAutoIncrement(int val) {
		// CHECKSTYLE:ON
		// do nothing by default
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _setFieldValue(String name, Object value) {
		// CHECKSTYLE:ON
		try {
			Field f = getClass().getDeclaredField(name);
			
			f.setAccessible(true);
			f.set(this, value);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
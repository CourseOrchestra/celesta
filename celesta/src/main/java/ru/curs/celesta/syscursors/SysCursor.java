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
		Celesta.getTriggerDispatcher().fireTrigger(TriggerType.PRE_DELETE, this);
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _postDelete() {
		// CHECKSTYLE:ON
		Celesta.getTriggerDispatcher().fireTrigger(TriggerType.POST_DELETE, this);
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _preUpdate() {
		// CHECKSTYLE:ON
		Celesta.getTriggerDispatcher().fireTrigger(TriggerType.PRE_UPDATE, this);
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _postUpdate() {
		// CHECKSTYLE:ON
		Celesta.getTriggerDispatcher().fireTrigger(TriggerType.POST_UPDATE, this);
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _preInsert() {
		// CHECKSTYLE:ON
		Celesta.getTriggerDispatcher().fireTrigger(TriggerType.PRE_INSERT, this);
	}

	@Override
	// CHECKSTYLE:OFF
	protected void _postInsert() {
		// CHECKSTYLE:ON
		Celesta.getTriggerDispatcher().fireTrigger(TriggerType.POST_INSERT, this);
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
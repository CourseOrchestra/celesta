package ru.curs.celesta.event;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.python.core.PyFunction;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.syscursors.SysCursor;

/**
 * Created by ioann on 31.05.2017.
 */
public class TriggerDispatcher {

	private final EnumMap<TriggerType, Map<String, List<PyFunction>>> triggerMap = new EnumMap<>(TriggerType.class);

	public TriggerDispatcher() {
		Arrays.stream(TriggerType.values()).forEach(t -> triggerMap.put(t, new HashMap<>()));
	}

	public void registerTrigger(TriggerType type, String tableName, PyFunction pyFunction) {
		Map<String, List<PyFunction>> tableMap = triggerMap.get(type);
		tableMap.computeIfAbsent(tableName, s -> new ArrayList<>()).add(pyFunction);
	}

	public void fireTrigger(TriggerType type, SysCursor cursor) {
		Map<String, List<PyFunction>> tableMap = triggerMap.get(type);
		String tableName;
		try {
			tableName = cursor.meta().getName();
		} catch (CelestaException e) {
			throw new RuntimeException(e);
		}
		List<PyFunction> handlers = tableMap.get(tableName);
		if (handlers != null) {
			Object[] args = { cursor };
			handlers.forEach(f -> f._jcall(args));
		}
	}

}

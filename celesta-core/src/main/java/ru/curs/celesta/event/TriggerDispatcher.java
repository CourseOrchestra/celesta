package ru.curs.celesta.event;

import java.util.*;
import java.util.function.Consumer;

import ru.curs.celesta.dbutils.Cursor;

/**
 * Created by ioann on 31.05.2017.
 */
public class TriggerDispatcher {

    private final EnumMap<TriggerType, Map<Class<? extends Cursor>, List<Consumer<Cursor>>>> triggerMap = new EnumMap<>(TriggerType.class);

    public TriggerDispatcher() {
        Arrays.stream(TriggerType.values()).forEach(t -> triggerMap.put(t, new HashMap<>()));
    }

    public void registerTrigger(TriggerType type, Class<? extends Cursor> cursorClass, Consumer consumer) {
        Map<Class<? extends Cursor>, List<Consumer<Cursor>>> cursorClassMap = triggerMap.get(type);
        cursorClassMap.computeIfAbsent(cursorClass, s -> new ArrayList<>()).add(consumer);
    }

    public void fireTrigger(TriggerType type, Cursor cursor) {
        Map<Class<? extends Cursor>, List<Consumer<Cursor>>> cursorClassMap = triggerMap.get(type);
        final Class<? extends Cursor> cursorClass = cursor.getClass();
        cursorClassMap.getOrDefault(cursorClass, Collections.emptyList())
                .forEach(consumer -> consumer.accept(cursor));
    }

}

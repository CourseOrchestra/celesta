package ru.curs.celesta.event;

import ru.curs.celesta.dbutils.Cursor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Created by ioann on 31.05.2017.
 */
public final class TriggerDispatcher {

    private final EnumMap<TriggerType, Map<Class<? extends Cursor>, List<Consumer<?>>>> triggerMap =
            new EnumMap<>(TriggerType.class);

    public TriggerDispatcher() {
        Arrays.stream(TriggerType.values()).forEach(t -> triggerMap.put(t, new HashMap<>()));
    }

    public <T extends Cursor> void registerTrigger(TriggerType type, Class<T> cursorClass, Consumer<? super T> consumer) {
        Map<Class<? extends Cursor>, List<Consumer<?>>> cursorClassMap = triggerMap.get(type);
        cursorClassMap.computeIfAbsent(cursorClass, s -> new ArrayList<>()).add(consumer);
    }

    @SuppressWarnings("unchecked")
    public void fireTrigger(TriggerType type, Cursor cursor) {
        Map<Class<? extends Cursor>, List<Consumer<?>>> cursorClassMap = triggerMap.get(type);
        final Class<? extends Cursor> cursorClass = cursor.getClass();
        List<Consumer<?>> triggers = cursorClassMap.getOrDefault(cursorClass, Collections.emptyList());
        triggers.forEach(consumer ->
                ((Consumer<? super Cursor>) consumer).accept(cursor));
    }

}

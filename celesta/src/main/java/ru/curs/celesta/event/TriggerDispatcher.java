package ru.curs.celesta.event;

import org.python.core.PyFunction;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.Table;
import ru.curs.celesta.syscursors.SysCursor;

import java.util.*;

/**
 * Created by ioann on 31.05.2017.
 */
public class TriggerDispatcher {

  private EnumMap<TriggerType, Map<String, List<PyFunction>>> triggerMap = new EnumMap<>(TriggerType.class);


  public TriggerDispatcher() {
    Arrays.stream(TriggerType.values()).forEach(
        t -> triggerMap.put(t, new HashMap<>())
    );
  }


  public void registerTrigger(TriggerType type, String tableName, PyFunction pyFunction) {
    Map<String, List<PyFunction>> tableMap = triggerMap.get(type);

    if (!tableMap.containsKey(tableName)) {
      tableMap.put(tableName, new ArrayList<>());
    }

    tableMap.get(tableName).add(pyFunction);
  }


  public void fireTrigger(TriggerType type, SysCursor cursor) {
    Map<String, List<PyFunction>> tableMap = triggerMap.get(type);

    try {
      String tableName = cursor.meta().getName();

      if (tableMap.containsKey(tableName)) {
        Object[] args = {cursor};
        tableMap.get(tableName).forEach(f -> f._jcall(args));
      }
    } catch (CelestaException e) {
      throw new RuntimeException(e);
    }

  }

}

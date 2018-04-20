package ru.curs.celesta.dbutils;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.PermissionDeniedException;
import ru.curs.celesta.score.MaterializedView;
import ru.curs.celesta.score.ParseException;


import java.util.List;
import java.util.Set;

/**
 * Created by ioann on 06.07.2017.
 */
public abstract class MaterializedViewCursor extends BasicCursor {

  private MaterializedView meta = null;
  private final CursorGetHelper getHelper;


  public MaterializedViewCursor(CallContext context) {
    super(context);

    CursorGetHelper.CursorGetHelperBuilder cghb = new CursorGetHelper.CursorGetHelperBuilder();
    cghb.withDb(db())
        .withConn(conn())
        .withMeta(meta())
        .withTableName(_objectName());

    getHelper = cghb.build();
  }

  public MaterializedViewCursor(CallContext context, Set<String> fields) {
    super(context, fields);

    CursorGetHelper.CursorGetHelperBuilder cghb = new CursorGetHelper.CursorGetHelperBuilder();
    cghb.withDb(db())
        .withConn(conn())
        .withMeta(meta())
        .withTableName(_objectName())
        .withFields(fieldsForStatement);

    getHelper = cghb.build();
  }

  /**
   * Описание представления (метаинформация).
   */
  @Override
  public MaterializedView meta() {
    if (meta == null)
      try {
        meta = callContext().getScore()
            .getGrain(_grainName()).getElement(_objectName(), MaterializedView.class);
      } catch (ParseException e) {
        throw new CelestaException(e.getMessage());
      }
    return meta;
  }


  @Override
  final void appendPK(List<String> l, List<Boolean> ol, Set<String> colNames) {
    // Всегда добавляем в конец OrderBy поля первичного ключа, идующие в
    // естественном порядке
    for (String colName : meta().getPrimaryKey().keySet())
      if (!colNames.contains(colName)) {
        l.add(String.format("\"%s\"", colName));
        ol.add(Boolean.FALSE);
      }
  }

  /**
   * Осуществляет поиск записи по ключевым полям, выбрасывает исключение, если
   * запись не найдена.
   *
   * @param values
   *            значения ключевых полей
   */
  public final void get(Object... values) {
    if (!tryGet(values)) {
      StringBuilder sb = new StringBuilder();
      for (Object value : values) {
        if (sb.length() > 0)
          sb.append(", ");
        sb.append(value == null ? "null" : value.toString());
      }
      throw new CelestaException("There is no %s (%s).", _objectName(), sb.toString());
    }
  }

  /**
   * Осуществляет поиск записи по ключевым полям, возвращает значение --
   * найдена запись или нет.
   *
   * @param values
   *            значения ключевых полей
   */
  public final boolean tryGet(Object... values) {
    if (!canRead())
      throw new PermissionDeniedException(callContext(), meta(), Action.READ);

    return getHelper.internalGet(this::_parseResult, this::initXRec,
        0, values);
  }


  /**
   * Получает из базы данных запись, соответствующую полям текущего первичного
   * ключа.
   */
  public final boolean tryGetCurrent() {
    if (!canRead())
      throw new PermissionDeniedException(callContext(), meta(), Action.READ);
    return getHelper.internalGet(this::_parseResult, this::initXRec,
        0, _currentKeyValues());
  }

  /**
   * Возвращает в массиве значения полей первичного ключа.
   */
  public Object[] getCurrentKeyValues() {
    return _currentKeyValues();
  }


  // CHECKSTYLE:OFF
	/*
	 * Эта группа методов именуется по правилам Python, а не Java. В Python
	 * имена protected-методов начинаются с underscore. Использование методов
	 * без underscore приводит к конфликтам с именами атрибутов.
	 */

  protected abstract Object[] _currentKeyValues();
}

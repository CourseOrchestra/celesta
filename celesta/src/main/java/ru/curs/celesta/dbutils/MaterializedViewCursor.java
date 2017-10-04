package ru.curs.celesta.dbutils;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.Celesta;
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


  public MaterializedViewCursor(CallContext context) throws CelestaException {
    super(context);

    CursorGetHelper.CursorGetHelperBuilder cghb = new CursorGetHelper.CursorGetHelperBuilder();
    cghb.withDb(db())
        .withConn(conn())
        .withMeta(meta())
        .withTableName(_tableName());

    getHelper = cghb.build();
  }

  public MaterializedViewCursor(CallContext context, Set<String> fields) throws CelestaException {
    super(context, fields);

    CursorGetHelper.CursorGetHelperBuilder cghb = new CursorGetHelper.CursorGetHelperBuilder();
    cghb.withDb(db())
        .withConn(conn())
        .withMeta(meta())
        .withTableName(_tableName())
        .withFields(fieldsForStatement);

    getHelper = cghb.build();
  }

  /**
   * Описание представления (метаинформация).
   *
   * @throws CelestaException
   *             в случае ошибки извлечения метаинформации (в норме не должна
   *             происходить).
   */
  @Override
  public MaterializedView meta() throws CelestaException {
    if (meta == null)
      try {
        meta = callContext().getScore()
            .getGrain(_grainName()).getElement(_tableName(), MaterializedView.class);
      } catch (ParseException e) {
        throw new CelestaException(e.getMessage());
      }
    return meta;
  }


  @Override
  final void appendPK(List<String> l, List<Boolean> ol, Set<String> colNames) throws CelestaException {
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
   * @throws CelestaException
   *             в случае, если запись не найдена
   */
  public final void get(Object... values) throws CelestaException {
    if (!tryGet(values)) {
      StringBuilder sb = new StringBuilder();
      for (Object value : values) {
        if (sb.length() > 0)
          sb.append(", ");
        sb.append(value == null ? "null" : value.toString());
      }
      throw new CelestaException("There is no %s (%s).", _tableName(), sb.toString());
    }
  }

  /**
   * Осуществляет поиск записи по ключевым полям, возвращает значение --
   * найдена запись или нет.
   *
   * @param values
   *            значения ключевых полей
   * @throws CelestaException
   *             SQL-ошибка
   */
  public final boolean tryGet(Object... values) throws CelestaException {
    if (!canRead())
      throw new PermissionDeniedException(callContext(), meta(), Action.READ);

    return getHelper.internalGet(this::_parseResult, this::initXRec,
        0, values);
  }


  /**
   * Получает из базы данных запись, соответствующую полям текущего первичного
   * ключа.
   *
   * @throws CelestaException
   *             Ошибка доступа или взаимодействия с БД.
   */
  public final boolean tryGetCurrent() throws CelestaException {
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

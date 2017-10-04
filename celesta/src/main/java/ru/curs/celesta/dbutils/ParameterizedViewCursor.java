package ru.curs.celesta.dbutils;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.Celesta;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.query.FromClause;
import ru.curs.celesta.dbutils.term.FromTerm;
import ru.curs.celesta.score.GrainElement;
import ru.curs.celesta.score.ParameterizedView;
import ru.curs.celesta.score.ParseException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by ioann on 15.08.2017.
 */
public abstract class ParameterizedViewCursor extends BasicCursor {

  private ParameterizedView meta = null;
  private Map<String, Object> parameters = null;


  public ParameterizedViewCursor(CallContext context, Map<String, Object> parameters) throws CelestaException {
    super(context);
    initParameters(parameters);
  }

  public ParameterizedViewCursor(CallContext context, Set<String> fields, Map<String, Object> parameters) throws CelestaException {
    super(context, fields);
    initParameters(parameters);
  }

  private void initParameters(Map<String, Object> parameters) throws CelestaException {
    if (!meta().getParameters().keySet().containsAll(parameters.keySet()))
      throw new CelestaException("Not all required parameters were passed");

    this.parameters = parameters;
  }

  /**
   * Описание представления (метаинформация).
   *
   * @throws CelestaException
   *             в случае ошибки извлечения метаинформации (в норме не должна
   *             происходить).
   */
  @Override
  public ParameterizedView meta() throws CelestaException {
    if (meta == null)
      try {
        meta = callContext().getScore()
            .getGrain(_grainName()).getElement(_tableName(), ParameterizedView.class);
      } catch (ParseException e) {
        throw new CelestaException(e.getMessage());
      }
    return meta;
  }

  @Override
  final void appendPK(List<String> l, List<Boolean> ol, Set<String> colNames) throws CelestaException {
    // для представлений мы сортируем всегда по первому столбцу, если
    // сортировки нет вообще
    if (colNames.isEmpty()) {
      l.add(String.format("\"%s\"", meta().getColumns().keySet().iterator().next()));
      ol.add(Boolean.FALSE);
    }
  }

  @Override
  protected FromClause getFrom() throws CelestaException {

    FromClause result = new FromClause();
    GrainElement ge = meta();

    result.setGe(ge);
    result.setExpression(db().getCallFunctionSql(meta));

    List paramValues = meta().getParameters().keySet().stream()
        .map(pName -> parameters.get(pName))
        .collect(Collectors.toList());
    result.setParameters(paramValues);

    return result;
  }
}

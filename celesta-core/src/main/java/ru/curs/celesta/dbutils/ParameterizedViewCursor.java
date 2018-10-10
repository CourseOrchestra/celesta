package ru.curs.celesta.dbutils;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.query.FromClause;
import ru.curs.celesta.exception.CelestaParseException;
import ru.curs.celesta.score.DataGrainElement;
import ru.curs.celesta.score.ParameterizedView;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by ioann on 15.08.2017.
 */
public abstract class ParameterizedViewCursor extends BasicCursor {

  private ParameterizedView meta = null;
  protected Map<String, Object> parameters = null;


  public ParameterizedViewCursor(CallContext context, Map<String, Object> parameters) {
    super(context);
    initParameters(parameters);
  }

  public ParameterizedViewCursor(CallContext context, Set<String> fields, Map<String, Object> parameters) {
    super(context, fields);
    initParameters(parameters);
  }

  private void initParameters(Map<String, Object> parameters) {
    if (!meta().getParameters().keySet().containsAll(parameters.keySet()))
      throw new CelestaException("Not all required parameters were passed");

    this.parameters = parameters;
  }

  /**
   * Описание представления (метаинформация).
   */
  @Override
  public ParameterizedView meta() {
    if (meta == null)
      try {
        meta = callContext().getScore()
            .getGrain(_grainName()).getElement(_objectName(), ParameterizedView.class);
      } catch (CelestaParseException e) {
        throw new CelestaException(e.getMessage());
      }
    return meta;
  }

  @Override
  final void appendPK(List<String> l, List<Boolean> ol, Set<String> colNames) {
    // для представлений мы сортируем всегда по первому столбцу, если
    // сортировки нет вообще
    if (colNames.isEmpty()) {
      l.add(String.format("\"%s\"", meta().getColumns().keySet().iterator().next()));
      ol.add(Boolean.FALSE);
    }
  }

  @Override
  protected FromClause getFrom() {

    FromClause result = new FromClause();
    DataGrainElement ge = meta();

    result.setGe(ge);
    result.setExpression(db().getCallFunctionSql(meta));

    List paramValues = meta().getParameters().keySet().stream()
        .map(pName -> parameters.get(pName))
        .collect(Collectors.toList());
    result.setParameters(paramValues);

    return result;
  }
}

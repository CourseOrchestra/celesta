package ru.curs.celesta.dbutils;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.query.FromClause;
import ru.curs.celesta.score.ColumnMeta;
import ru.curs.celesta.score.DataGrainElement;
import ru.curs.celesta.score.ParameterizedView;
import ru.curs.celesta.score.ParseException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Parameterized view cursor.
 *
 * @author ioann
 * @since 2017-08-15
 */
public abstract class ParameterizedViewCursor extends BasicCursor {

  private ParameterizedView meta = null;
  protected Map<String, Object> parameters = null;


  public ParameterizedViewCursor(CallContext context, Map<String, Object> parameters) {
    super(context);
    initParameters(parameters);
  }

  public ParameterizedViewCursor(CallContext context, Map<String, Object> parameters, ColumnMeta<?>... columns) {
      this(context, Arrays.stream(columns).map(ColumnMeta::getName).collect(Collectors.toSet()), parameters);
  }

  public ParameterizedViewCursor(CallContext context, Set<String> fields, Map<String, Object> parameters) {
    super(context, fields);
    initParameters(parameters);
  }

  /**
   * Creates a parameterized view specific cursor.
   *
   * @param view  Cursor related view
   * @param callContext  Call context that is used for cursor creation
   * @param parameters   A map with parameterizing parameters
   * @return
   */
  public static ParameterizedViewCursor create(ParameterizedView view, CallContext callContext,
          Map<String, Object> parameters) {
      try {
          return (ParameterizedViewCursor) getCursorClass(view).getConstructor(CallContext.class, Map.class)
                  .newInstance(callContext, parameters);
      } catch (ReflectiveOperationException ex) {
          throw new CelestaException("Cursor creation failed for grain element: " + view.getName(), ex);
      }
  }

  /**
   * Creates a parameterized view specific cursor.
   *
   * @param view  Cursor related parameterized view
   * @param callContext  Call context that is used for cursor creation
   * @param fields  Fields the cursor should operate on
   * @param parameters   A map with parameterizing parameters
   * @return
   */
  public static ParameterizedViewCursor create(ParameterizedView view, CallContext callContext,
          Set<String> fields, Map<String, Object> parameters) {
      try {
          return (ParameterizedViewCursor) getCursorClass(view).getConstructor(CallContext.class, Set.class, Map.class)
                  .newInstance(callContext, fields, parameters);
      } catch (ReflectiveOperationException ex) {
          throw new CelestaException("Cursor creation failed for grain element: " + view.getName(), ex);
      }
  }

  private void initParameters(Map<String, Object> parameters) {
    if (!meta().getParameters().keySet().containsAll(parameters.keySet())) {
      throw new CelestaException("Not all required parameters were passed");
    }

    this.parameters = parameters;
  }

  /**
   * Returns parameterized view description (meta information).
   *
   * @return
   */
  @Override
  public ParameterizedView meta() {
    if (meta == null) {
      try {
        meta = callContext().getScore()
            .getGrain(_grainName()).getElement(_objectName(), ParameterizedView.class);
      } catch (ParseException e) {
        throw new CelestaException(e.getMessage());
      }
    }

    return meta;
  }

  @Override
  final void appendPK(List<String> l, List<Boolean> ol, final Set<String> colNames) {
    // The views are always sorted by the first column if there's no sorting at all.
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

    List<Object> paramValues = meta().getParameters().keySet().stream()
        .map(pName -> parameters.get(pName))
        .collect(Collectors.toList());
    result.setParameters(paramValues);

    return result;
  }

}

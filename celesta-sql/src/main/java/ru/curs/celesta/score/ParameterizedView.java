package ru.curs.celesta.score;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Parameterized View object in metadata.
 *
 * @author ioann
 * @since 2017-08-09
 */
public final class ParameterizedView extends View {

  private final Map<String, Parameter> parameters = new LinkedHashMap<>();
  private final List<String> parameterRefsWithOrder = new ArrayList<>();

  public ParameterizedView(GrainPart grainPart, String name) throws ParseException {
    super(grainPart, name);
  }

  @Override
  String viewType() {
    return "function";
  }


  @Override
  void finalizeWhereConditionParsing() throws ParseException {
    List<TableRef> t = new ArrayList<>(getTables().values());
    if (whereCondition != null) {
      whereCondition.resolveFieldRefs(t);
      ParameterResolverResult paramResolveResult = whereCondition.resolveParameterRefs(parameters);

      if (!paramResolveResult.getUnusedParameters().isEmpty()) {
        String unusedParametersStr = String.join(", ", paramResolveResult.getUnusedParameters());
        throw new ParseException(String.format("%s '%s' contains not used parameters %s.",
            viewType(), getName(), unusedParametersStr));
      }
      whereCondition.validateTypes();

      parameterRefsWithOrder.addAll(paramResolveResult.getParametersWithUsageOrder());
    }
  }

  /**
   * Adds a parameter to the view.
   *
   * @param parameter  parameter
   * @throws ParseException  if parameter name is empty or parameter already exists in the view
   */
  public void addParameter(Parameter parameter) throws ParseException {
    if (parameter == null) {
      throw new IllegalArgumentException();
    }

    if (parameter.getName() == null || parameter.getName().isEmpty()) {
      throw new ParseException(String.format("%s '%s' contains a parameter with undefined name.",
          viewType(), getName()));
    }

    if (parameters.containsKey(parameter.getName())) {
      throw new ParseException(
          String.format("%s '%s' already contains parameter with name '%s'. Use unique names for %s parameters.",
              viewType(), getName(), parameter.getName(), viewType())
      );
    }

    parameters.put(parameter.getName(), parameter);
  }

  /**
   * Returns a map <b>parameter name</b> -> <b>parameter</b>.
   *
   * @return
   */
  public Map<String, Parameter> getParameters() {
    return Collections.unmodifiableMap(parameters);
  }

  public List<String> getParameterRefsWithOrder() {
    return parameterRefsWithOrder;
  }

}

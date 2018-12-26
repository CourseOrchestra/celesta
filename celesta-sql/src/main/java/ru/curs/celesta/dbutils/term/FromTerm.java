package ru.curs.celesta.dbutils.term;

import ru.curs.celesta.dbutils.QueryBuildingHelper;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;

import java.util.List;

/**
 * @author ioann
 * @since 2017-08-16
 */
public class FromTerm {

  private final List<?> values;

  public FromTerm(List<?> values) {
    this.values = values;
  }

  public void programParams(List<ParameterSetter> program, QueryBuildingHelper queryBuildingHelper) {
    values.forEach(
        v -> program.add(ParameterSetter.createArbitrary(v, queryBuildingHelper))
    );
  }
}

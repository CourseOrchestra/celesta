package ru.curs.celesta.dbutils.query;

import ru.curs.celesta.score.DataGrainElement;

import java.util.ArrayList;
import java.util.List;

/**
 * Data holder for FROM clause.
 *
 * @author ioann
 * @since 2017-08-15
 */
public final class FromClause {
  private DataGrainElement ge;
  private String expression;
  private  List<Object> parameters = new ArrayList<>();

  /**
   * Returns grain element.
   *
   * @return  {@link DataGrainElement}
   */
  public DataGrainElement getGe() {
    return ge;
  }

  /**
   * Sets grain element.
   *
   * @param ge  {@link DataGrainElement}
   */
  public void setGe(DataGrainElement ge) {
    this.ge = ge;
  }

  /**
   * Returns FROM expression.
   *
   * @return
   */
  public String getExpression() {
    return expression;
  }

  /**
   * Sets FROM expression.
   *
   * @param expression  FROM expression
   */
  public void setExpression(String expression) {
    this.expression = expression;
  }

  public List<Object> getParameters() {
    return parameters;
  }

  public void setParameters(List<Object> parameters) {
    this.parameters = parameters;
  }

}

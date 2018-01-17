package ru.curs.celesta.dbutils.query;

import ru.curs.celesta.score.DataGrainElement;
import ru.curs.celesta.score.GrainElement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ioann on 15.08.2017.
 */
public class FromClause {
  DataGrainElement ge;
  String expression;
  List<Object> parameters = new ArrayList<>();

  public DataGrainElement getGe() {
    return ge;
  }

  public void setGe(DataGrainElement ge) {
    this.ge = ge;
  }

  public String getExpression() {
    return expression;
  }

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

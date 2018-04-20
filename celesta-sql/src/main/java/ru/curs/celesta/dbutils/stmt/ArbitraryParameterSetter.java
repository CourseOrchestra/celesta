package ru.curs.celesta.dbutils.stmt;

import ru.curs.celesta.dbutils.QueryBuildingHelper;

import java.sql.PreparedStatement;

/**
 * Created by ioann on 16.08.2017.
 */
public class ArbitraryParameterSetter extends ParameterSetter {

  private final Object v;

  public ArbitraryParameterSetter(QueryBuildingHelper queryBuildingHelper, Object v) {
    super(queryBuildingHelper);
    this.v = v;
  }

  @Override
  public void execute(PreparedStatement stmt, int paramNum, Object[] rec, int recversion)  {
    setParam(stmt, paramNum, v);
  }
}

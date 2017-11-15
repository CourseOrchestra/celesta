package ru.curs.celesta.dbutils.stmt;

import ru.curs.celesta.CelestaException;

import java.sql.PreparedStatement;

/**
 * Created by ioann on 16.08.2017.
 */
public class ArbitraryParameterSetter extends ParameterSetter {

  private final Object v;

  public ArbitraryParameterSetter(Object v) {
    this.v = v;
  }

  @Override
  public void execute(PreparedStatement stmt, int paramNum, Object[] rec, int recversion) throws CelestaException {
    setParam(stmt, paramNum, v);
  }
}

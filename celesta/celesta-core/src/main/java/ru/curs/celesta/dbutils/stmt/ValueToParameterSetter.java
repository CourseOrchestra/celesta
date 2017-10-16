package ru.curs.celesta.dbutils.stmt;

import ru.curs.celesta.CelestaException;

import ru.curs.celesta.dbutils.filter.Range;

import java.sql.PreparedStatement;

/**
 * Created by ioann on 10.05.2017.
 */
public final class ValueToParameterSetter extends ParameterSetter {
  private final Range r;

  ValueToParameterSetter(Range r) {
    this.r = r;
  }

  @Override
  public void execute(PreparedStatement stmt, int paramNum, Object[] rec, int recversion) throws CelestaException {
    setParam(stmt, paramNum, r.getValueTo());
  }

}

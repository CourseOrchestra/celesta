package ru.curs.celesta.dbutils.term;

import ru.curs.celesta.dbutils.stmt.ParameterSetter;

import java.util.List;

/**
 * Created by ioann on 16.08.2017.
 */
public class FromTerm {

  private final List values;

  public FromTerm(List values) {
    this.values = values;
  }

  public void programParams(List<ParameterSetter> program) {
    values.forEach(
        v -> program.add(ParameterSetter.createArbitrary(v))
    );
  }
}

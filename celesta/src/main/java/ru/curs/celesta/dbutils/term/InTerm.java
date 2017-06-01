package ru.curs.celesta.dbutils.term;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.filter.In;
import ru.curs.celesta.dbutils.filter.value.FieldsLookup;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;
import ru.curs.celesta.score.Table;

import java.util.List;

/**
 * Created by ioann on 01.06.2017.
 */
public final class InTerm extends WhereTerm {

  private final In filter;

  public InTerm(In filter) {
    this.filter = filter;
  }

  @Override
  public String getWhere() throws CelestaException {

    FieldsLookup lookup = filter.getLookup();
    String template = "( %s ) IN (SELECT %s FROM %s )";

    String fieldsStr = String.join(",", lookup.getFields());
    String otherFieldsStr = String.join(",", lookup.getOtherFields());

    Table otherTable = lookup.getOtherCursor().meta();
    String otherTableStr = String.format("%s.%s", otherTable.getGrain().getName(), otherTable.getName());

    String result = String.format(template, fieldsStr, otherFieldsStr, otherTableStr);
    return result;
  }

  @Override
  public void programParams(List<ParameterSetter> program) throws CelestaException {
    //DO NOTHING
  }
}

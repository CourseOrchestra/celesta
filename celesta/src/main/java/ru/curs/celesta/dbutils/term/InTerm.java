package ru.curs.celesta.dbutils.term;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.dbutils.filter.In;
import ru.curs.celesta.dbutils.filter.value.FieldsLookup;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;

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
    DBAdaptor db = DBAdaptor.getAdaptor();

    final String otherWhere;
    if (filter.getOtherwhereTerm() != null) {
      otherWhere = filter.getOtherwhereTerm().getWhere();
    } else {
      otherWhere = "";
    }

    return db.getInFilterClause(lookup.getTable(), lookup.getOtherTable(),
        lookup.getFields(), lookup.getOtherFields(), otherWhere);
  }

  @Override
  public void programParams(List<ParameterSetter> program) throws CelestaException {
    if (filter.getOtherwhereTerm() != null) {
      filter.getOtherwhereTerm().programParams(program);
    }
  }

}

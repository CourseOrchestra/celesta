package ru.curs.celesta.dbutils.term;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.QueryBuildingHelper;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.dbutils.filter.In;
import ru.curs.celesta.dbutils.filter.value.FieldsLookup;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by ioann on 01.06.2017.
 */
public final class InTerm extends WhereTerm {

  private final In filter;
  private final QueryBuildingHelper queryBuildingHelper;

  public InTerm(In filter, QueryBuildingHelper queryBuildingHelper) {
    this.filter = filter;
    this.queryBuildingHelper = queryBuildingHelper;
  }

  @Override
  public String getWhere() throws CelestaException {
    return filter.getLookupWhereTermMap().entrySet().stream()
        .map(e -> buildWhereLookup(e.getKey(), e.getValue()))
        .collect(Collectors.joining(" AND "));
  }

  @Override
  public void programParams(List<ParameterSetter> program) throws CelestaException {
    for (WhereTermsMaker wtm : filter.getOtherWhereTermMakers())
      if (wtm != null) {
        wtm.getWhereTerm().programParams(program);
      }
  }


  private String buildWhereLookup(FieldsLookup lookup, WhereTermsMaker whereTermsMaker) {
    try {

      final String otherWhere;
      if (whereTermsMaker != null) {
        otherWhere = whereTermsMaker.getWhereTerm().getWhere();
      } else {
        otherWhere = "";
      }

      return queryBuildingHelper.getInFilterClause(lookup.getTable(), lookup.getOtherTable(),
          lookup.getFields(), lookup.getOtherFields(), otherWhere);
    } catch (CelestaException e) {
      throw new RuntimeException(e);
    }
  }

}

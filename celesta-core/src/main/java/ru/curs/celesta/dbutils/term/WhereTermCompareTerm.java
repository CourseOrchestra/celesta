package ru.curs.celesta.dbutils.term;

import ru.curs.celesta.dbutils.QueryBuildingHelper;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;

import java.util.List;

public class WhereTermCompareTerm extends WhereTerm {

    private final WhereTerm lTerm;
    private final WhereTerm rTerm;
    private final String op;

    public WhereTermCompareTerm(WhereTerm lTerm, WhereTerm rTerm, String op) {
        this.lTerm = lTerm;
        this.rTerm = rTerm;
        this.op = op;
    }

    @Override
    public String getWhere() {
        return String.format("%s %s %s", lTerm.getWhere(), op, rTerm.getWhere());
    }

    @Override
    public void programParams(List<ParameterSetter> program, QueryBuildingHelper queryBuildingHelper) {
        lTerm.programParams(program, queryBuildingHelper);
        rTerm.programParams(program, queryBuildingHelper);
    }
}

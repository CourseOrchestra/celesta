package ru.curs.celesta.dbutils.term;

import ru.curs.celesta.dbutils.QueryBuildingHelper;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;

import java.util.List;
import java.util.stream.Collectors;

public class ValuesCortegeTerm extends WhereTerm {

    final List<Integer> fieldIndices;

    public ValuesCortegeTerm(List<Integer> fieldIndices) {
        this.fieldIndices = fieldIndices;
    }

    @Override
    public String getWhere() {
        String placeHolders = fieldIndices.stream()
                .map((i) -> "?")
                .collect(Collectors.joining(", "));

        return String.format("(%s)", placeHolders);
    }

    @Override
    public void programParams(List<ParameterSetter> program, QueryBuildingHelper queryBuildingHelper) {
        fieldIndices.forEach(
                (i) -> program.add(ParameterSetter.create(i, queryBuildingHelper))
        );
    }

}

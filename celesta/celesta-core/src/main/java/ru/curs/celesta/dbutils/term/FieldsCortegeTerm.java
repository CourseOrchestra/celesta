package ru.curs.celesta.dbutils.term;

import ru.curs.celesta.dbutils.stmt.ParameterSetter;

import java.util.List;

public class FieldsCortegeTerm extends WhereTerm {
    // quoted column names
    final List<String> fieldNames;

    public FieldsCortegeTerm(List<String> fieldNames) {
        this.fieldNames = fieldNames;
    }

    @Override
    public String getWhere() {
        return String.format("(%s)", String.join(", ", fieldNames));
    }

    @Override
    public void programParams(List<ParameterSetter> program) {
        //do nothing
    }
}

package ru.curs.celesta.dbutils.term;

import ru.curs.celesta.dbutils.QueryBuildingHelper;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;

import java.util.List;

/**
 * FALSE constant.
 */
public final class AlwaysFalse extends WhereTerm {

    /**
     * FALSE constant singleton.
     */
    public static final AlwaysFalse FALSE = new AlwaysFalse();

    private AlwaysFalse() {
    };

    @Override
    public String getWhere() {
        return "(1 = 0)";
    }

    @Override
    public void programParams(List<ParameterSetter> program, QueryBuildingHelper queryBuildingHelper) {
        // do nothing, no parameters
    }
}

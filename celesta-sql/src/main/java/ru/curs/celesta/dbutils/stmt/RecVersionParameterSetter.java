package ru.curs.celesta.dbutils.stmt;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.QueryBuildingHelper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Parameter setter for recverion parameter.
 */
public final class RecVersionParameterSetter extends ParameterSetter {

    public RecVersionParameterSetter(QueryBuildingHelper queryBuildingHelper) {
        super(queryBuildingHelper);
    }

    @Override
    public void execute(PreparedStatement stmt, int paramNum, Object[] rec, int recversion)  {
        try {
            stmt.setInt(paramNum, recversion);
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage(), e);
        }
    }

}

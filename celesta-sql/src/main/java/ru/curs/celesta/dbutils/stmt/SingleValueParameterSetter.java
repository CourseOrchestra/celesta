package ru.curs.celesta.dbutils.stmt;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.QueryBuildingHelper;
import ru.curs.celesta.dbutils.filter.SingleValue;

import java.sql.PreparedStatement;

/**
 * Parameter setter for single value filter.
 */
public final class SingleValueParameterSetter extends ParameterSetter {
	private final SingleValue v;

	public SingleValueParameterSetter(QueryBuildingHelper queryBuildingHelper, SingleValue v) {
		super(queryBuildingHelper);
		this.v = v;
	}

	@Override
	public void execute(PreparedStatement stmt, int paramNum, Object[] rec, int recversion) throws CelestaException {
		setParam(stmt, paramNum, v.getValue());
	}
}

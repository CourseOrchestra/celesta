package ru.curs.celesta.dbutils.stmt;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.filter.SingleValue;

import java.sql.PreparedStatement;

/**
 * Parameter setter for single value filter.
 */
public final class SingleValueParameterSetter extends ParameterSetter {
	private final SingleValue v;

	SingleValueParameterSetter(SingleValue v) {
		this.v = v;
	}

	@Override
	public void execute(PreparedStatement stmt, int paramNum, Object[] rec, int recversion) throws CelestaException {
		setParam(stmt, paramNum, v.getValue());
	}
}

package ru.curs.celesta.dbutils.stmt;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.filter.Range;

import java.sql.PreparedStatement;

/**
 * Parameter setter for 'from' part of range filter.
 */
public final class ValueFromParameterSetter extends ParameterSetter {
	private final Range r;

	ValueFromParameterSetter(Range r) {
		this.r = r;
	}

	@Override
	public void execute(PreparedStatement stmt, int paramNum, Object[] rec, int recversion) throws CelestaException {
		setParam(stmt, paramNum, r.getValueFrom());
	}
}

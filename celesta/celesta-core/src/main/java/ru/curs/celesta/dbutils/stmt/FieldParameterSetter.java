package ru.curs.celesta.dbutils.stmt;

import ru.curs.celesta.CelestaException;

import java.sql.PreparedStatement;

/**
 * Parameter setter for record field.
 */
public final class FieldParameterSetter extends ParameterSetter {
	private static final FieldParameterSetter[] CACHE = new FieldParameterSetter[16];

	static {
		for (int i = 0; i < CACHE.length; i++) {
			CACHE[i] = new FieldParameterSetter(i);
		}
	}

	private final int fieldNum;

	private FieldParameterSetter(int fieldNum) {
		this.fieldNum = fieldNum;
	}

	static FieldParameterSetter get(int i) {
		if (i < 0)
			throw new IllegalArgumentException();
		if (i < CACHE.length)
			return CACHE[i];
		return new FieldParameterSetter(i);
	}

	@Override
	public void execute(PreparedStatement stmt, int paramNum, Object[] rec, int recversion) throws CelestaException {
		setParam(stmt, paramNum, rec[fieldNum]);
	}

}

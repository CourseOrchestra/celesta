package ru.curs.celesta.dbutils.stmt;

import ru.curs.celesta.CelestaException;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Parameter setter for recverion parameter.
 */
public final class RecversionParameterSetter extends ParameterSetter {

	public static final RecversionParameterSetter THESETTER = new RecversionParameterSetter();

	private RecversionParameterSetter() {
	}

	@Override
	public void execute(PreparedStatement stmt, int paramNum, Object[] rec, int recversion) throws CelestaException {
		try {
			stmt.setInt(paramNum, recversion);
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
	}

}

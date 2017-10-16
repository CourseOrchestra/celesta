package ru.curs.celesta.dbutils.stmt;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.*;
import ru.curs.celesta.dbutils.filter.Range;
import ru.curs.celesta.dbutils.filter.SingleValue;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

/**
 * An element of parameter setting program.
 */
public abstract class ParameterSetter {

	public abstract void execute(PreparedStatement stmt, int paramNum, Object[] rec, int recversion) throws CelestaException;

	protected static void setParam(PreparedStatement stmt, int i, Object v) throws CelestaException {
		try {
			if (v == null)
				stmt.setNull(i, java.sql.Types.NULL);
			else if (v instanceof Integer)
				stmt.setInt(i, (Integer) v);
			else if (v instanceof Double)
				stmt.setDouble(i, (Double) v);
			else if (v instanceof String)
				stmt.setString(i, (String) v);
			else if (v instanceof Boolean)
				stmt.setBoolean(i, (Boolean) v);
			else if (v instanceof Date) {
				Timestamp d = new Timestamp(((Date) v).getTime());
				stmt.setTimestamp(i, d);
			} else if (v instanceof BLOB) {
				stmt.setBinaryStream(i, ((BLOB) v).getInStream(), ((BLOB) v).size());
			}
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
	}

	public static ParameterSetter create(int i) {
		return FieldParameterSetter.get(i);
	}

	public static ParameterSetter create(SingleValue v) {
		return new SingleValueParameterSetter(v);
	}

	public static ParameterSetter createForValueFrom(Range r) {
		return new ValueFromParameterSetter(r);
	}

	public static ParameterSetter createForValueTo(Range r) {
		return new ValueToParameterSetter(r);
	}

	public static ParameterSetter createForRecversion() {
		return RecversionParameterSetter.THESETTER;
	}

	public static ArbitraryParameterSetter createArbitrary(Object v) {
		return new ArbitraryParameterSetter(v);
	}
}

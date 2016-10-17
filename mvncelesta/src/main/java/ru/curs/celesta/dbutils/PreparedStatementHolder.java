package ru.curs.celesta.dbutils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import ru.curs.celesta.CelestaException;

/**
 * A container for parameterized prepared statement.
 */
abstract class PreparedStmtHolder {
	private PreparedStatement stmt;
	private final List<ParameterSetter> program = new LinkedList<>();

	boolean isStmtValid() throws CelestaException {
		try {
			return !(stmt == null || stmt.isClosed());
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
	}

	/**
	 * Returns prepared statement with refreshed parameters.
	 * 
	 * @param rec
	 *            Array of record fields' values.
	 * 
	 * @throws CelestaException
	 *             something wrong.
	 */
	synchronized PreparedStatement getStatement(Object[] rec, int recversion) throws CelestaException {
		if (!isStmtValid()) {
			program.clear();
			stmt = initStatement(program);
			// everything should be initialized at this point
			if (!isStmtValid())
				throw new IllegalStateException();
		}
		int i = 1;
		for (ParameterSetter f : program) {
			f.execute(stmt, i++, rec, recversion);
		}
		return stmt;
	}

	synchronized void close() {
		try {
			if (!(stmt == null))
				stmt.close();
		} catch (SQLException e) {
			e = null;
		}
		stmt = null;
		program.clear();
	}

	protected abstract PreparedStatement initStatement(List<ParameterSetter> program) throws CelestaException;

}

/**
 * Holder for a statement which depends on nulls mask.
 */
abstract class MaskedStatementHolder extends PreparedStmtHolder {
	private int[] nullsMaskIndices;
	private boolean[] nullsMask;

	@Override
	synchronized PreparedStatement getStatement(Object[] rec, int recversion) throws CelestaException {
		reusable: if (isStmtValid()) {
			for (int i = 0; i < nullsMask.length; i++) {
				if (nullsMask[i] != (rec[nullsMaskIndices[i]] == null)) {
					close();
					break reusable;
				}
			}
			return super.getStatement(rec, recversion);
		}
		nullsMaskIndices = getNullsMaskIndices();
		nullsMask = new boolean[nullsMaskIndices.length];
		for (int i = 0; i < nullsMask.length; i++) {
			nullsMask[i] = rec[nullsMaskIndices[i]] == null;
		}

		return super.getStatement(rec, recversion);
	}

	@Override
	synchronized void close() {
		super.close();
		nullsMaskIndices = null;
	}

	protected boolean[] getNullsMask() {
		return nullsMask;
	}

	protected abstract int[] getNullsMaskIndices() throws CelestaException;

}

/**
 * An element of parameter setting program.
 */
abstract class ParameterSetter {

	abstract void execute(PreparedStatement stmt, int paramNum, Object[] rec, int recversion) throws CelestaException;

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

	static ParameterSetter create(int i) {
		return FieldParameterSetter.get(i);
	}

	static ParameterSetter create(SingleValue v) {
		return new SingleValueParameterSetter(v);
	}

	static ParameterSetter createForValueFrom(Range r) {
		return new ValueFromParameterSetter(r);
	}

	static ParameterSetter createForValueTo(Range r) {
		return new ValueToParameterSetter(r);
	}

	public static ParameterSetter createForRecversion() {
		return RecversionParameterSetter.THESETTER;
	}
}

/**
 * Parameter setter for single value filter.
 */
final class SingleValueParameterSetter extends ParameterSetter {
	private final SingleValue v;

	SingleValueParameterSetter(SingleValue v) {
		this.v = v;
	}

	@Override
	void execute(PreparedStatement stmt, int paramNum, Object[] rec, int recversion) throws CelestaException {
		setParam(stmt, paramNum, v.getValue());
	}
}

/**
 * Parameter setter for 'from' part of range filter.
 */
final class ValueFromParameterSetter extends ParameterSetter {
	private final Range r;

	ValueFromParameterSetter(Range r) {
		this.r = r;
	}

	@Override
	void execute(PreparedStatement stmt, int paramNum, Object[] rec, int recversion) throws CelestaException {
		setParam(stmt, paramNum, r.getValueFrom());
	}
}

/**
 * Parameter setter for 'to' part of range filter.
 */
final class ValueToParameterSetter extends ParameterSetter {
	private final Range r;

	ValueToParameterSetter(Range r) {
		this.r = r;
	}

	@Override
	void execute(PreparedStatement stmt, int paramNum, Object[] rec, int recversion) throws CelestaException {
		setParam(stmt, paramNum, r.getValueTo());
	}
}

/**
 * Parameter setter for recverion parameter.
 */
final class RecversionParameterSetter extends ParameterSetter {

	public static final RecversionParameterSetter THESETTER = new RecversionParameterSetter();

	private RecversionParameterSetter() {
	}

	@Override
	void execute(PreparedStatement stmt, int paramNum, Object[] rec, int recversion) throws CelestaException {
		try {
			stmt.setInt(paramNum, recversion);
		} catch (SQLException e) {
			throw new CelestaException(e.getMessage());
		}
	}

}

/**
 * Parameter setter for record field.
 */
final class FieldParameterSetter extends ParameterSetter {
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
	void execute(PreparedStatement stmt, int paramNum, Object[] rec, int recversion) throws CelestaException {
		setParam(stmt, paramNum, rec[fieldNum]);
	}

}
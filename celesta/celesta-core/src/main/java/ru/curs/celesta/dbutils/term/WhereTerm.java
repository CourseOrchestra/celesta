package ru.curs.celesta.dbutils.term;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;

import java.util.List;

/**
 * A term of filter/navigation where clause.
 */
public abstract class WhereTerm {
	public abstract String getWhere() throws CelestaException;

	public abstract void programParams(List<ParameterSetter> program) throws CelestaException;
}

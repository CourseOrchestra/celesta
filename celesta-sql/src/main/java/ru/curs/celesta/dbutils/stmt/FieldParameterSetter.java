package ru.curs.celesta.dbutils.stmt;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.QueryBuildingHelper;

import java.sql.PreparedStatement;


/**
 * Parameter setter for record field.
 */
public final class FieldParameterSetter extends ParameterSetter {

	private final int fieldNum;

	public FieldParameterSetter(QueryBuildingHelper queryBuildingHelper, int fieldNum) {
		super(queryBuildingHelper);
		this.fieldNum = fieldNum;
	}

	@Override
	public void execute(PreparedStatement stmt, int paramNum, Object[] rec, int recversion) throws CelestaException {
		setParam(stmt, paramNum, rec[fieldNum]);
	}

}

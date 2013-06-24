package ru.curs.celesta.dbutils;

/**
 * Адаптер MSSQL.
 * 
 */
class MSSQLAdaptor extends DBAdaptor {

	@Override
	public boolean tableExists(String schema, String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean userTablesExist() {
		// TODO Auto-generated method stub
		return false;
	}

}
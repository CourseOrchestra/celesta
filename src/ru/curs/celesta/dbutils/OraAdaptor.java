package ru.curs.celesta.dbutils;

/**
 * Адаптер Ora.
 * 
 */

class OraAdaptor extends DBAdaptor {

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
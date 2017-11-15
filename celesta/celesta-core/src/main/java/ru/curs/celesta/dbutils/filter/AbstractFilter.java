package ru.curs.celesta.dbutils.filter;

/**
 * Внутреннее представление фильтра на поле.
 */
public abstract class AbstractFilter {
	public abstract boolean filterEquals(AbstractFilter f);
}


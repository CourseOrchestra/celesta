package ru.curs.celesta.dbutils.meta;

import ru.curs.celesta.score.Index;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Информация об индексе, полученная из метаданых базы данных.
 */
public final class DBIndexInfo {
	private final String tableName;
	private final String indexName;
	private final List<String> columnNames = new LinkedList<>();

	public DBIndexInfo(String tableName, String indexName) {
		this.tableName = tableName;
		this.indexName = indexName;
	}

	public String getTableName() {
		return tableName;
	}

	public String getIndexName() {
		return indexName;
	}

	public List<String> getColumnNames() {
		return columnNames;
	}

	@Override
	public String toString() {
		return String.format("%s.%s", tableName, indexName);
	}

	public boolean reflects(Index ind) {
		boolean result = ind.getName().equals(indexName) && ind.getTable().getName().equals(tableName);
		if (!result)
			return false;
		Collection<String> dbIndexCols = columnNames;
		Collection<String> metaIndexCols = ind.getColumns().keySet();
		Iterator<String> i1 = dbIndexCols.iterator();
		Iterator<String> i2 = metaIndexCols.iterator();
		result = dbIndexCols.size() == metaIndexCols.size();
		if (!result)
			return false;
		while (i1.hasNext() && result) {
			result = i1.next().equals(i2.next()) && result;
		}
		return result;
	}
}

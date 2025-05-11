package ru.curs.celesta.dbutils.meta;

import ru.curs.celesta.score.Index;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Index information taken from metadata of the database.
 */
public final class DbIndexInfo {
    private final String tableName;
    private final String indexName;
    private final boolean isUnique;
    private final List<String> columnNames = new LinkedList<>();

    public DbIndexInfo(String tableName, String indexName, boolean isUnique) {
        this.tableName = tableName;
        this.indexName = indexName;
        this.isUnique = isUnique;
    }

    /**
     * Table name for which index is defined.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Index name.
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * If the index is unique.
     */
    public boolean isUnique() {
        return isUnique;
    }

    /**
     * Column names of the index.
     */
    public List<String> getColumnNames() {
        return columnNames;
    }

    @Override
    public String toString() {
        return String.format("%s.%s", tableName, indexName);
    }

    public boolean reflects(Index ind) {
        boolean result = ind.getName().equals(indexName)
                && ind.getTable().getName().equals(tableName)
                && ind.isUnique() == isUnique;
        if (!result) {
            return false;
        }
        Collection<String> dbIndexCols = columnNames;
        Collection<String> metaIndexCols = ind.getColumns().keySet();
        Iterator<String> i1 = dbIndexCols.iterator();
        Iterator<String> i2 = metaIndexCols.iterator();
        result = dbIndexCols.size() == metaIndexCols.size();
        if (!result) {
            return false;
        }
        while (i1.hasNext() && result) {
            result = i1.next().equals(i2.next());
        }
        return result;
    }

}

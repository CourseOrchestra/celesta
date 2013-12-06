package ru.curs.celesta.dbutils;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import ru.curs.celesta.score.BooleanColumn;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.DateTimeColumn;
import ru.curs.celesta.score.FKBehaviour;
import ru.curs.celesta.score.FloatingColumn;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.StringColumn;
import ru.curs.celesta.score.Table;

/*
 * Модуль содержит классы с мета-информацией, извлекаемой из базы данных.
 */

/**
 * Данные о колонке в базе данных в виде, необходимом для Celesta.
 */
final class DBColumnInfo {
	private String name;
	private Class<? extends Column> type;
	private boolean isNullable;
	private String defaultValue = "";
	private int length;
	private boolean isMax;
	private boolean isIdentity;

	String getName() {
		return name;
	}

	Class<? extends Column> getType() {
		return type;
	}

	boolean isNullable() {
		return isNullable;
	}

	String getDefaultValue() {
		return defaultValue;
	}

	int getLength() {
		return length;
	}

	boolean isMax() {
		return isMax;
	}

	boolean isIdentity() {
		return isIdentity;
	}

	void setName(String name) {
		this.name = name;
	}

	void setType(Class<? extends Column> type) {
		this.type = type;
	}

	void setNullable(boolean isNullable) {
		this.isNullable = isNullable;
	}

	void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}

	void setLength(int length) {
		this.length = length;
	}

	void setMax(boolean isMax) {
		this.isMax = isMax;
	}

	void setIdentity(boolean isIdentity) {
		this.isIdentity = isIdentity;
	}

	boolean reflects(Column value) {
		// Если тип не совпадает -- дальше не проверяем.
		if (value.getClass() != type)
			return false;

		// Проверяем nullability, но помним о том, что в Oracle DEFAULT
		// ''-строки всегда nullable
		if (type != StringColumn.class || !"''".equals(defaultValue)) {
			if (value.isNullable() != isNullable)
				return false;
		}

		if (type == IntegerColumn.class) {
			// Если свойство IDENTITY не совпадает -- не проверяем
			if (isIdentity != ((IntegerColumn) value).isIdentity())
				return false;
		} else if (type == StringColumn.class) {
			// Если параметры длин не совпали -- не проверяем
			StringColumn col = (StringColumn) value;
			if (!(isMax ? col.isMax() : length == col.getLength()))
				return false;
		}

		// Если в данных пустой default, а в метаданных -- не пустой -- то
		// не проверяем
		if (defaultValue.isEmpty())
			return value.getDefaultValue() == null;

		// Случай непустого default-значения в данных.
		return checkDefault(value);
	}

	private boolean checkDefault(Column value) {
		boolean result;
		if (type == BooleanColumn.class) {
			try {
				result = BooleanColumn.parseSQLBool(defaultValue).equals(
						value.getDefaultValue());
			} catch (ParseException e) {
				result = false;
			}
		} else if (type == IntegerColumn.class) {
			result = Integer.valueOf(defaultValue).equals(
					value.getDefaultValue());
		} else if (type == FloatingColumn.class) {
			result = Double.valueOf(defaultValue).equals(
					value.getDefaultValue());
		} else if (type == DateTimeColumn.class) {
			if ("GETDATE()".equalsIgnoreCase(defaultValue))
				result = ((DateTimeColumn) value).isGetdate();
			else {
				try {
					result = DateTimeColumn.parseISODate(defaultValue).equals(
							value.getDefaultValue());
				} catch (ParseException e) {
					result = false;
				}
			}
		} else if (type == StringColumn.class) {
			result = defaultValue.equals(StringColumn
					.quoteString(((StringColumn) value).getDefaultValue()));
		} else {
			result = defaultValue.equals(value.getDefaultValue());
		}
		return result;
	}
}

/**
 * Информация об индексе, полученная из метаданых базы данных.
 */
final class DBIndexInfo {
	private final String tableName;
	private final String indexName;
	private final int hash;

	DBIndexInfo(String tableName, String indexName) {
		this.tableName = tableName;
		this.indexName = indexName;
		hash = Integer.rotateLeft(tableName.hashCode(), 3)
				^ indexName.hashCode();
	}

	String getTableName() {
		return tableName;
	}

	String getIndexName() {
		return indexName;
	}

	@Override
	public int hashCode() {
		return hash;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof DBIndexInfo) {
			DBIndexInfo ii = (DBIndexInfo) obj;
			return tableName.equals(ii.tableName)
					&& indexName.equals(ii.indexName);
		}
		return super.equals(obj);
	}

	@Override
	public String toString() {
		return String.format("%s.%s", tableName, indexName);
	}

}

/**
 * Информация о первичном ключе, полученная из метаданных базы данных.
 */
final class DBPKInfo {
	private String name;
	private final List<String> columnNames = new LinkedList<>();

	void addColumnName(String columnName) {
		columnNames.add(columnName);
	}

	void setName(String name) {
		this.name = name;
	}

	String getName() {
		return name;
	}

	List<String> getColumnNames() {
		return columnNames;
	}

	boolean isEmpty() {
		return columnNames.isEmpty();
	}

	boolean reflects(Table t) {
		boolean result = t.getPkConstraintName().equals(name)
				&& (columnNames.size() == t.getPrimaryKey().size());
		Iterator<String> i1 = t.getPrimaryKey().keySet().iterator();
		Iterator<String> i2 = columnNames.iterator();
		while (result && i1.hasNext()) {
			result = i1.next().equals(i2.next());
		}
		return result;
	}
}

/**
 * Информация о внешнем ключе, полученная из базы данных.
 */
final class DBFKInfo {

	private String tableName;
	private String name;
	private String refGrainName;
	private String refTableName;
	private FKBehaviour deleteBehaviour = FKBehaviour.NO_ACTION;
	private FKBehaviour updateBehaviour = FKBehaviour.NO_ACTION;
	private final List<String> columnNames = new LinkedList<>();

	public DBFKInfo(String name) {
		this.name = name;
	}

	String getName() {
		return name;
	}

	String getTableName() {
		return tableName;
	}

	void setTableName(String tableName) {
		this.tableName = tableName;
	}

	String getRefTableName() {
		return refTableName;
	}

	void setRefTableName(String refTableName) {
		this.refTableName = refTableName;
	}

	List<String> getColumnNames() {
		return columnNames;
	}

	String getRefGrainName() {
		return refGrainName;
	}

	void setRefGrainName(String refGrainName) {
		this.refGrainName = refGrainName;
	}

	FKBehaviour getDeleteBehaviour() {
		return deleteBehaviour;
	}

	void setDeleteBehaviour(FKBehaviour deleteBehaviour) {
		this.deleteBehaviour = deleteBehaviour;
	}

	FKBehaviour getUpdateBehaviour() {
		return updateBehaviour;
	}

	void setUpdateBehaviour(FKBehaviour updateBehaviour) {
		this.updateBehaviour = updateBehaviour;
	}

}
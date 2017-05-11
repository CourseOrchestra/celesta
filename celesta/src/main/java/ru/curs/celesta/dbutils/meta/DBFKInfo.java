package ru.curs.celesta.dbutils.meta;

import ru.curs.celesta.score.FKRule;
import ru.curs.celesta.score.ForeignKey;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Информация о внешнем ключе, полученная из базы данных.
 */
public final class DBFKInfo {

	private String tableName;
	private String name;
	private String refGrainName;
	private String refTableName;
	private FKRule deleteRule = FKRule.NO_ACTION;
	private FKRule updateRule = FKRule.NO_ACTION;
	private final List<String> columnNames = new LinkedList<>();

	public DBFKInfo(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getRefTableName() {
		return refTableName;
	}

	public void setRefTableName(String refTableName) {
		this.refTableName = refTableName;
	}

	public List<String> getColumnNames() {
		return columnNames;
	}

	public String getRefGrainName() {
		return refGrainName;
	}

	public void setRefGrainName(String refGrainName) {
		this.refGrainName = refGrainName;
	}

	public FKRule getDeleteRule() {
		return deleteRule;
	}

	public void setDeleteRule(FKRule deleteBehaviour) {
		this.deleteRule = deleteBehaviour;
	}

	public FKRule getUpdateRule() {
		return updateRule;
	}

	public void setUpdateRule(FKRule updateBehaviour) {
		this.updateRule = updateBehaviour;
	}

	public boolean reflects(ForeignKey fk) {
		if (!fk.getParentTable().getName().equals(tableName))
			return false;
		if (!fk.getReferencedTable().getGrain().getName().equals(refGrainName))
			return false;
		if (!fk.getReferencedTable().getName().equals(refTableName))
			return false;
		if (fk.getColumns().size() != columnNames.size())
			return false;
		Iterator<String> i1 = fk.getColumns().keySet().iterator();
		Iterator<String> i2 = columnNames.iterator();
		while (i1.hasNext()) {
			if (!i1.next().equals(i2.next()))
				return false;
		}
		if (!fk.getDeleteRule().equals(deleteRule))
			return false;
		if (!fk.getUpdateRule().equals(updateRule))
			return false;
		return true;
	}

}

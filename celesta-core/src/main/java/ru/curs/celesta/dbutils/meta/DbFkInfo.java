package ru.curs.celesta.dbutils.meta;

import ru.curs.celesta.score.FKRule;
import ru.curs.celesta.score.ForeignKey;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Information on foreign key taken from the database.
 */
public final class DbFkInfo {

    private String tableName;
    private final String name;
    private String refGrainName;
    private String refTableName;
    private FKRule deleteRule = FKRule.NO_ACTION;
    private FKRule updateRule = FKRule.NO_ACTION;
    private final List<String> columnNames = new LinkedList<>();

    public DbFkInfo(String name) {
        this.name = name;
    }

    /**
     * Returns foreign key name.
     *
     * @return
     */
    public String getName() {
        return name;
    }

    /**
     * Returns table name for which foreign key is defined.
     *
     * @return
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Sets table name for which foreign key is defined.
     *
     * @param tableName  table name
     * @return
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Returns table name that foreign key refers to.
     *
     * @return
     */
    public String getRefTableName() {
        return refTableName;
    }

    /**
     * Sets name of a table that the foreign key refers to.
     *
     * @param refTableName  referenced table name
     */
    public void setRefTableName(String refTableName) {
        this.refTableName = refTableName;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    /**
     * Returns referenced grain name.
     *
     * @return
     */
    public String getRefGrainName() {
        return refGrainName;
    }

    /**
     * Set referenced grain name.
     *
     * @param refGrainName  referenced grain name
     */
    public void setRefGrainName(String refGrainName) {
        this.refGrainName = refGrainName;
    }

    /**
     * Returns foreign key rule type on deletion.
     *
     * @return
     */
    public FKRule getDeleteRule() {
        return deleteRule;
    }

    /**
     * Sets foreign key rule type on deletion.
     *
     * @param deleteBehaviour  deletion rule type
     */
    public void setDeleteRule(FKRule deleteBehaviour) {
        this.deleteRule = deleteBehaviour;
    }

    /**
     * Returns foreign key rule type on update.
     *
     * @return
     */
    public FKRule getUpdateRule() {
        return updateRule;
    }

    /**
     * Sets foreign key rule type on update.
     *
     * @param updateBehaviour  update rule type
     */
    public void setUpdateRule(FKRule updateBehaviour) {
        this.updateRule = updateBehaviour;
    }

    public boolean reflects(ForeignKey fk) {
        if (!fk.getParentTable().getName().equals(tableName)) {
            return false;
        }
        if (!fk.getReferencedTable().getGrain().getName().equals(refGrainName)) {
            return false;
        }
        if (!fk.getReferencedTable().getName().equals(refTableName)) {
            return false;
        }
        if (fk.getColumns().size() != columnNames.size()) {
            return false;
        }
        Iterator<String> i1 = fk.getColumns().keySet().iterator();
        Iterator<String> i2 = columnNames.iterator();
        while (i1.hasNext()) {
            if (!i1.next().equals(i2.next())) {
                return false;
            }
        }
        if (!fk.getDeleteRule().equals(deleteRule)) {
            return false;
        }
        if (!fk.getUpdateRule().equals(updateRule)) {
            return false;
        }
        return true;
    }

}

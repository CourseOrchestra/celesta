package ru.curs.celesta.score;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * Table index. Celesta permits only creation of simple indices without
 * UNIQUE restriction.
 */
public class Index extends GrainElement implements HasColumns {
    private static final String INDEX_CREATION_ERROR = "Error while creating index '%s': column '%s' in table '%s' is ";
    private final Table table;
    private final NamedElementHolder<Column> columns = new NamedElementHolder<Column>() {
        @Override
        protected String getErrorMsg(String name) {
            return String.format("Column '%s' is defined more than once in index '%s'", name, getName());
        }
    };

    Index(GrainPart grainPart, String tableName, String name) throws ParseException {
        super(grainPart, name);
        if (tableName == null || name == null) {
            throw new IllegalArgumentException();
        }
        table = getGrain().getElement(tableName, Table.class);
        if (table == null) {
            throw new ParseException(
                    String.format("Error while creating index '%s': table '%s' not found.", name, tableName));
        }
        getGrain().addIndex(this);
        table.addIndex(this);
    }

    public Index(Table t, String name, String[] columns) throws ParseException {
        this(t.getGrainPart(), t.getName(), name);
        for (String n : columns) {
            addColumn(n);
        }
        finalizeIndex();
    }

    /**
     * Returns table of the index.
     *
     * @return
     */
    public Table getTable() {
        return table;
    }

    /**
     * Adds a column to the index.
     *
     * @param columnName  column name (such column should exist in the table)
     * @throws ParseException  in case if the column is not found, or is already
     *                         available in the index, or is of type IMAGE
     */
    void addColumn(String columnName) throws ParseException {
        if (columnName == null) {
            throw new IllegalArgumentException();
        }
        Column c = table.getColumns().get(columnName);
        if (c == null) {
            throw new ParseException(
                    String.format(INDEX_CREATION_ERROR + "not defined.", getName(), columnName, table.getName()));
        }
        if (c instanceof BinaryColumn) {
            throw new ParseException(String.format(
                    INDEX_CREATION_ERROR + "of long binary type and therefore cannot be a part of an index.", getName(),
                    columnName, table.getName()));
        }
        if (c instanceof StringColumn && ((StringColumn) c).isMax()) {
            throw new ParseException(
                    String.format(INDEX_CREATION_ERROR + "of TEXT type and therefore cannot be a part of an index.",
                            getName(), columnName, table.getName()));
        }

        if (c.isNullable()) {
            String err = String.format(
                    "WARNING for index '%s': column '%s' in table '%s' is nullable and this can affect performance.",
                    getName(), columnName, table.getName());
            System.out.println(err);
        }

        columns.addElement(c);
    }

    /**
     * Finalizes the index.
     *
     * @throws ParseException  in case if there's already an index on the table
     *                         that duplicates the set of fields of this index.
     */
    void finalizeIndex() throws ParseException {
        if (Arrays.equals(
                getColumns().entrySet().toArray(),
                table.getPrimaryKey().entrySet().toArray()
        )) {
            throw new ParseException(
                    String.format("Can't add index %s to table %s.%s. "
                                  + "Primary key with same columns and order already exists.",
                            getName(), table.getGrain().getName(), table.getName())
            );
        }
        // Ищем дублирующиеся по составу полей индексы
        for (Index ind : getGrain().getIndices().values()) {
            if (ind == this) {
                continue;
            }
            if (ind.table != table) {
                continue;
            }
            if (ind.columns.size() != columns.size()) {
                continue;
            }
            Iterator<Column> i = ind.columns.iterator();
            boolean coincide = true;
            for (Column c : columns) {
                if (c != i.next()) {
                    coincide = false;
                    break;
                }
            }
            if (coincide) {
                throw new ParseException(
                        String.format("Error while creating index '%s': it is duplicate of index '%s' for table '%s'",
                                getName(), ind.getName(), table.getName()));
            }
        }
    }

    /**
     * Returns columns of the index.
     *
     * @return
     */
    public Map<String, Column> getColumns() {
        return columns.getElements();
    }

    /**
     * Deletes the index.
     *
     * @throws ParseException  when trying to change the system grain
     */
    public void delete() throws ParseException {
        getGrain().removeIndex(this);
    }

    @Override
    public int getColumnIndex(String name) {
        return columns.getIndex(name);
    }

}

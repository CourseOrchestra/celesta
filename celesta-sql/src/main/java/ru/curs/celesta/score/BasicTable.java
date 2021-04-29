package ru.curs.celesta.score;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONException;

import ru.curs.celesta.CelestaException;

/**
 * Super class for {@link Table} and {@link ReadOnlyTable} that encompasses common logic
 * for both table types.
 *
 * @author Pavel Perminov (packpaul@mail.ru)
 * @since 2019-07-14
 */
public abstract class BasicTable extends DataGrainElement implements TableElement {

    final NamedElementHolder<Column<?>> pk = new NamedElementHolder<Column<?>>() {
        @Override
        protected String getErrorMsg(String name) {
            return String.format("Column '%s' defined more than once for primary key in table '%s'.", name, getName());
        }

    };

    private final NamedElementHolder<Column<?>> columns = new NamedElementHolder<Column<?>>() {
        @Override
        protected String getErrorMsg(String name) {
            return String.format("Column '%s' defined more than once in table '%s'.", name, getName());
        }

    };

    private final boolean canHaveEmptyPK;

    private final Set<ForeignKey> fKeys = new LinkedHashSet<>();

    private final Set<Index> indices = new HashSet<>();

    private boolean pkFinalized = false;

    private boolean autoUpdate = true;

    private String pkConstraintName;

    protected BasicTable(GrainPart grainPart, String name, boolean canHaveEmptyPK) throws ParseException {
        super(grainPart, name);
        getGrain().addElement(this);
        this.canHaveEmptyPK = canHaveEmptyPK;
    }

    /**
     * Unmodified list of table columns.
     */
    @Override
    public Map<String, Column<?>> getColumns() {
        return columns.getElements();
    }


    @Override
    public final Column<?> getColumn(String colName) throws ParseException {
        Column<?> result = columns.get(colName);
        if (result == null) {
            throw new ParseException(
                    String.format("Column '%s' not found in table '%s.%s'", colName, getGrain().getName(), getName()));
        }
        return result;
    }

    @Override
    public final Map<String, Column<?>> getPrimaryKey() {
        return pk.getElements();
    }

    /**
     * Adds a column to the table.
     *
     * @param column  new column
     * @throws ParseException  if a column with the same name is already defined
     */
    @Override
    public void addColumn(Column<?> column) throws ParseException {
        if (column.getParentTable() != this) {
            throw new IllegalArgumentException();
        }
        getGrain().modify();
        columns.addElement(column);
    }

    @Override
    public final String toString() {
        return "name: " + getName() + " " + columns.toString();
    }

    /**
     * Sets primary key on the table in a form of array of columns.
     * It is used for dynamic metadata management.
     *
     * @param columnNames
     *            array of columns
     * @throws ParseException
     *            in case when an empty array is passed in
     */
    public void setPK(String... columnNames) throws ParseException {
        if (columnNames == null || columnNames.length == 0 && !canHaveEmptyPK) {
            throw new ParseException(
                    String.format("Primary key for table %s.%s cannot be empty.", getGrain().getName(), getName()));
        }
        for (String n : columnNames) {
            validatePKColumn(n);
        }
        getGrain().modify();
        pk.clear();
        pkFinalized = false;
        for (String n : columnNames) {
            addPK(n);
        }
        finalizePK();
    }

    /**
     * Adds a column of the primary key.
     *
     * @param name  primary key column name
     */
    void addPK(String name) throws ParseException {
        name = getGrain().getScore().getIdentifierParser().parse(name);
        if (pkFinalized) {
            throw new ParseException(String.format("More than one PRIMARY KEY definition in table '%s'.", getName()));
        }
        Column<?> c = validatePKColumn(name);
        pk.addElement(c);
    }

    private Column<?> validatePKColumn(String name) throws ParseException {
        if (VersionedElement.REC_VERSION.equals(name)) {
            throw new ParseException(String.format("Column '%s' is not allowed for primary key.", name));
        }
        Column<?> c = columns.get(name);
        if (c == null) {
            throw new ParseException(String.format("Column '%s' is not defined in table '%s'.", name, getName()));
        }
        if (c.isNullable()) {
            throw new ParseException(String.format(
                    "Column '%s' is nullable and therefore it cannot be " + "a part of a primary key in table '%s'.",
                    name, getName()));
        }
        if (c instanceof BinaryColumn) {
            throw new ParseException(String.format("Column %s is of long binary type and therefore "
                    + "it cannot a part of a primary key in table '%s'.", name, getName()));
        }
        if (c instanceof StringColumn && ((StringColumn) c).isMax()) {
            throw new ParseException(String.format(
                    "Column '%s' is of TEXT type and therefore " + "it cannot a part of a primary key in table '%s'.",
                    name, getName()));
        }

        return c;
    }

    final void addFK(ForeignKey fk) throws ParseException {
        if (fk.getParentTable() != this) {
            throw new IllegalArgumentException();
        }
        if (fKeys.contains(fk)) {
            StringBuilder sb = new StringBuilder();
            for (Column<?> c : fk.getColumns().values()) {
                if (sb.length() != 0) {
                    sb.append(", ");
                }
                sb.append(c.getName());
            }
            throw new ParseException(String.format("Foreign key with columns %s is already defined in table '%s'",
                    sb.toString(), getName()));
        }
        getGrain().modify();
        fKeys.add(fk);
    }

    synchronized final void removeFK(ForeignKey foreignKey) throws ParseException {
        getGrain().modify();
        fKeys.remove(foreignKey);
    }


    @Override
    public synchronized final void removeColumn(Column<?> column) throws ParseException {
        // It's not allowed to delete compound part of the primary key
        if (pk.contains(column)) {
            throw new ParseException(
                    String.format(YOU_CANNOT_DROP_A_COLUMN_THAT_BELONGS_TO + "a primary key. Change primary key first.",
                            getGrain().getName(), getName(), column.getName()));
        }
        // It's not allowed to delete compound part of an index
        for (Index ind : getGrain().getIndices().values()) {
            if (ind.getColumns().containsValue(column)) {
                throw new ParseException(String.format(
                        YOU_CANNOT_DROP_A_COLUMN_THAT_BELONGS_TO + "an index. Drop or change relevant index first.",
                        getGrain().getName(), getName(), column.getName()));
            }
        }
        // It's not allowed to delete compound part of a foreign key
        for (ForeignKey fk : fKeys) {
            if (fk.getColumns().containsValue(column)) {
                throw new ParseException(String.format(
                        YOU_CANNOT_DROP_A_COLUMN_THAT_BELONGS_TO
                                + "a foreign key. Drop or change relevant foreign key first.",
                        getGrain().getName(), getName(), column.getName()));
            }
        }
        getGrain().modify();
        columns.remove(column);
    }

    /**
     * Finalizes the creation of the primary key.
     *
     * @throws ParseException  if the primary key is empty.
     */
    public void finalizePK() throws ParseException {
        if (pk.isEmpty() && !canHaveEmptyPK) {
            throw new ParseException(String.format("No primary key defined for table %s!", getName()));
        }
        pkFinalized = true;
    }

    /**
     * Returns a set of foreign keys for the table.
     *
     * @return
     */
    public Set<ForeignKey> getForeignKeys() {
        return Collections.unmodifiableSet(fKeys);
    }

    /**
     * Returns a set of indices for the table.
     *
     * @return
     */
    public Set<Index> getIndices() {
        return Collections.unmodifiableSet(indices);
    }

    @Override
    public final String getPkConstraintName() {
        return pkConstraintName == null ? limitName("pk_" + getName()) : pkConstraintName;
    }

    /**
     * Sets the name of constraint for the primary key.
     *
     * @param pkConstraintName  PK constraint name
     * @throws ParseException  incorrect name
     */
    public void setPkConstraintName(String pkConstraintName) throws ParseException {
        if (pkConstraintName != null) {
            pkConstraintName = getGrain().getScore().getIdentifierParser().parse(pkConstraintName);
        }
        this.pkConstraintName = pkConstraintName;
    }

    /**
     * Whether autoupdate is on/off.<br/>
     * <br/>
     * {@code false} value indicates that the table was created with the option
     * WITH NO STRUCTURE UPDATE and it won't take part in the DB autoupdate.
     * Default is {@code true}.
     *
     * @return
     */
    public boolean isAutoUpdate() {
        return autoUpdate;
    }

    /**
     * Sets or clears the option WITH NO STRUCTURE UPDATE.
     *
     * @param autoUpdate
     *            {@code true} if the table is updated automatically,
     *            {@code false} - in the opposite case.
     */
    public void setAutoUpdate(boolean autoUpdate) {
        this.autoUpdate = autoUpdate;
    }

    @Override
    public final int getColumnIndex(String name) {
        return columns.getIndex(name);
    }

    final void addIndex(Index index) {
        indices.add(index);
    }

    final void removeIndex(Index index) {
        indices.remove(index);
    }

    /**
     * Returns interfaces that are implemented by the cursor (values of
     * 'implements' property) for this table.
     *
     * @return
     */
    public List<String> getImplements() {
        try {
            return CelestaDocUtils.getList(getCelestaDoc(), CelestaDocUtils.IMPLEMENTS);
        } catch (JSONException e1) {
            throw new CelestaException("Error in CelestaDoc for %s.%s: %s", getGrain().getName(),
                    getName(), e1.getMessage());
        }
    }

}

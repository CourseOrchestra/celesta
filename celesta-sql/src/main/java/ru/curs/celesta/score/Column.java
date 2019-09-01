package ru.curs.celesta.score;

import java.util.List;

import org.json.JSONException;

import ru.curs.celesta.CelestaException;

/**
 * Base class for describing a table column. Subclasses of this class correspond
 * to different column types.
 *
 * @param <V>  Java class of column value
 */
public abstract class Column<V> extends NamedElement implements ColumnMeta<V> {

    private final TableElement parentTable;
    private boolean nullable = true;

    Column(TableElement parentTable, String name) throws ParseException {
        super(name, parentTable.getGrain().getScore().getIdentifierParser());
        if (VersionedElement.REC_VERSION.equals(name)) {
            throw new ParseException(
                    String.format("Column name '%s' is reserved for system needs.", VersionedElement.REC_VERSION)
            );
        }
        this.parentTable = parentTable;
        parentTable.addColumn(this);
    }

    /**
     * Special version of constructor for construction of <code>recversion</code> field.
     *
     * @param parentTable  parent table (this is not added to the list of columns)
     * @throws ParseException  should not happen.
     */
    Column(TableElement parentTable) throws ParseException {
        super(VersionedElement.REC_VERSION, parentTable.getGrain().getScore().getIdentifierParser());
        this.parentTable = parentTable;
        nullable = false;
        setDefault("1");
    }

    /**
     * Returns options (the value of <code>option</code> property) for current field.
     * It is applicable only for text and Integer fields.
     *
     * @return
     *
     * @throws CelestaException  in case if options are provided incorrectly.
     */
    public List<String> getOptions()  {
        try {
            return CelestaDocUtils.getList(getCelestaDoc(), CelestaDocUtils.OPTION);
        } catch (JSONException e1) {
            throw new CelestaException("Error in CelestaDoc for %s.%s.%s: %s", getParentTable().getGrain().getName(),
                    getParentTable().getName(), getName(), e1.getMessage());
        }
    }

    /**
     * Sets default value.
     *
     * @param lexvalue  new value in string (lexical) format.
     * @throws ParseException  if format of default value is incorrect.
     */
    protected abstract void setDefault(String lexvalue) throws ParseException;

    @Override
    public String toString() {
        return getName();
    }

    /**
     * Returns table that current column belongs to.
     *
     * @return
     */
    public final TableElement getParentTable() {
        return parentTable;
    }

    /**
     * Sets property Nullable and default value.
     *
     * @param nullable  property Nullable
     * @param defaultValue  default value
     * @throws ParseException  in case if DEFAULT value has an incorrect format.
     */
    public final void setNullableAndDefault(boolean nullable, String defaultValue) throws ParseException {
        parentTable.getGrain().modify();
        String buf = defaultValue;
        this.nullable = nullable;
        setDefault(buf);
    }

    /**
     * Returns the value of Nullable property.
     *
     * @return
     */
    public final boolean isNullable() {
        return nullable;
    }

    /**
     * Deletes the column.
     *
     * @throws ParseException  if a part of the primary key, or a foreign key, or an index
     *                         is being deleted.
     */
    public final void delete() throws ParseException {
        parentTable.removeColumn(this);
    }

    /**
     * Returns default value.
     *
     * @return
     */
    public abstract V getDefaultValue();

    /**
     * DEFAULT value of the field in CelestaSQL language.
     */
    public abstract String getCelestaDefault();

}

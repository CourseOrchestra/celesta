package ru.curs.celesta.score;

/**
 * Table object in metadata.
 */
public final class Table extends BasicTable implements VersionedElement {

    private boolean versioned = true;

    private final IntegerColumn recVersion = new IntegerColumn(this);

    public Table(GrainPart grainPart, String name) throws ParseException {
        super(grainPart, name, false);
    }

    @Override
    public boolean hasPrimeKey() {
        return true;
    }

    /**
     * Whether the table is read only (WITH READ ONLY).
     *
     * @deprecated left for backwards compatibility.
     *
     * @return  always {@code false}
     */
    @Deprecated
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public boolean isVersioned() {
        return versioned;
    }

    /**
     * Sets to the table option "versioned".
     *
     * @param versioned  "versioned" option value
     */
    public void setVersioned(boolean versioned) {
        this.versioned = versioned;
    }

    @Override
    public IntegerColumn getRecVersionField() {
        return recVersion;
    }

}

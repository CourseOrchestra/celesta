package ru.curs.celesta.score;


/**
 * Reference to a table column.
 */
public final class FieldRef extends Expr {
    private String tableNameOrAlias;
    private String columnName;
    private Column<?> column = null;
    private ViewColumnMeta<?> meta;

    public FieldRef(String tableNameOrAlias, String columnName) throws ParseException {
        if (columnName == null) {
            throw new IllegalArgumentException();
        }
        this.tableNameOrAlias = tableNameOrAlias;
        this.columnName = columnName;

    }

    /**
     * Returns table name or alias.
     *
     * @return
     */
    public String getTableNameOrAlias() {
        return tableNameOrAlias;
    }

    /**
     * Returns column name.
     *
     * @return
     */
    public String getColumnName() {
        return columnName;
    }

    @Override
    public ViewColumnMeta<?> getMeta() {
        if (meta == null) {
            if (column != null) {
                if (column instanceof IntegerColumn) {
                    meta = new ViewColumnMeta<>(ViewColumnType.INT);
                } else if (column instanceof FloatingColumn) {
                    meta = new ViewColumnMeta<>(ViewColumnType.REAL);
                } else if (column instanceof DecimalColumn) {
                    meta = new ViewColumnMeta<>(ViewColumnType.DECIMAL);
                } else if (column instanceof StringColumn) {
                    StringColumn sc = (StringColumn) column;
                    if (sc.isMax()) {
                        meta = new ViewColumnMeta<>(ViewColumnType.TEXT);
                    } else {
                        meta = new ViewColumnMeta<>(ViewColumnType.TEXT, sc.getLength());
                    }
                } else if (column instanceof BooleanColumn) {
                    meta = new ViewColumnMeta<>(ViewColumnType.BIT);
                } else if (column instanceof DateTimeColumn) {
                    meta = new ViewColumnMeta<>(ViewColumnType.DATE);
                } else if (column instanceof ZonedDateTimeColumn) {
                    meta = new ViewColumnMeta<>(ViewColumnType.DATE_WITH_TIME_ZONE);
                } else if (column instanceof BinaryColumn) {
                    meta = new ViewColumnMeta<>(ViewColumnType.BLOB);
                    // This should not happen unless we introduced new types in
                    // Celesta
                } else {
                    throw new IllegalStateException();
                }
                meta.setNullable(column.isNullable());
                meta.setCelestaDoc(column.getCelestaDoc());
            } else {
                return new ViewColumnMeta<>(ViewColumnType.UNDEFINED);
            }
        }
        return meta;
    }

    /**
     * Returns the column that the reference is pointing to.
     *
     * @return
     */
    public Column<?> getColumn() {
        return column;
    }

    /**
     * Sets the column of the reference.
     *
     * @param column  reference column
     */
    void setColumn(Column<?> column) {
        this.column = column;
    }

    void setTableNameOrAlias(String tableNameOrAlias) {
        this.tableNameOrAlias = tableNameOrAlias;
    }

    void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public void accept(ExprVisitor visitor) throws ParseException {
        visitor.visitFieldRef(this);
    }

}

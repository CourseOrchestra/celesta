package ru.curs.celesta.score;

import java.util.ArrayList;
import java.util.List;

/**
 * Table builder class that is used by generated {@link ru.curs.celesta.score.CelestaParser} as
 * a workaround to overcome some shortcomings of JavaCC.
 *
 * @author Pavel Perminov (packpaul@mail.ru)
 * @since 2019-07-16
 */
final class TableBuilder {

    private final GrainPart grainPart;
    private final String name;

    private List<BuildAction> actions = new ArrayList<>();

    private BasicTable table;
    private boolean isReadOnly;

    TableBuilder(BasicTable table) {
        this(table.getGrainPart(), table.getName());
        this.table = table;
    }

    TableBuilder(GrainPart grainPart, String name) {
        this.grainPart = grainPart;
        this.name = name;
    }

    public Grain getGrain() {
        return this.grainPart.getGrain();
    }

    public BasicTable build() throws ParseException {

        if (this.table == null) {
            this.table = isReadOnly ? new ReadOnlyTable(grainPart, name) : new Table(grainPart, name);
        }

        for (BuildAction action : actions) {
            action.execute();
        }

        this.table.finalizePK();

        return this.table;
    }

    public void setCelestaDocLexem(String celestaDoc) {
        actions.add(() -> table.setCelestaDocLexem(celestaDoc));
    }

    public void setVersioned(boolean isVersioned) {
        actions.add(() -> {
            if (!isReadOnly) {
                ((Table) table).setVersioned(isVersioned);
            }
        });
    }

    public void setReadOnly(boolean isReadOnly) {
        this.isReadOnly = isReadOnly;
    }

    public void setAutoUpdate(boolean isAutoUpdate) {
        actions.add(() -> table.setAutoUpdate(isAutoUpdate));
    }

    public void addPK(String name) {
        actions.add(() -> table.addPK(name));
    }

    public void finalizePK() {
        actions.add(() -> table.finalizePK());
    }

    public void setPkConstraintName(String pkConstraintName) {
        actions.add(() -> table.setPkConstraintName(pkConstraintName));
    }

    public ColumnBuilder integerColumn(String columnName) {
        ColumnBuilder cb = new ColumnBuilder(columnName);
        actions.add(() -> cb.column = new IntegerColumn(table, columnName));
        return cb;
    }

    public ColumnBuilder floatingColumn(String columnName) {
        ColumnBuilder cb = new ColumnBuilder(columnName);
        actions.add(() -> cb.column = new FloatingColumn(table, columnName));
        return cb;
    }

    public ColumnBuilder decimalColumn(String columnName, int precision, int scale) {
        ColumnBuilder cb = new ColumnBuilder(columnName);
        actions.add(() -> cb.column = new DecimalColumn(table, columnName, precision, scale));
        return cb;
    }

    public ColumnBuilder stringColumn(String columnName) {
        ColumnBuilder cb = new ColumnBuilder(columnName);
        actions.add(() -> cb.column = new StringColumn(table, columnName));
        return cb;
    }

    public ColumnBuilder binaryColumn(String columnName) {
        ColumnBuilder cb = new ColumnBuilder(columnName);
        actions.add(() -> cb.column = new BinaryColumn(table, columnName));
        return cb;
    }

    public ColumnBuilder zonedDateTimeColumn(String columnName) {
        ColumnBuilder cb = new ColumnBuilder(columnName);
        actions.add(() -> cb.column = new ZonedDateTimeColumn(table, columnName));
        return cb;
    }

    public ColumnBuilder dateTimeColumn(String columnName) {
        ColumnBuilder cb = new ColumnBuilder(columnName);
        actions.add(() -> cb.column = new DateTimeColumn(table, columnName));
        return cb;
    }

    public ColumnBuilder booleanColumn(String columnName) {
        ColumnBuilder cb = new ColumnBuilder(columnName);
        actions.add(() -> cb.column = new BooleanColumn(table, columnName));
        return cb;
    }

    public ForeignKeyBuilder foreignKey() {
        ForeignKeyBuilder fkb = new ForeignKeyBuilder();
        actions.add(() -> fkb.foreignKey = new ForeignKey(table));
        return fkb;
    }

    @FunctionalInterface
    private interface BuildAction {
        void execute() throws ParseException;
    }

    public final class ColumnBuilder {
        private final String columnName;
        private Column<?> column;

        private ColumnBuilder(String columnName) {
            this.columnName = columnName;
        }

        public String getName() {
            return columnName;
        }

        public void setLength(String length) {
            actions.add(() -> ((StringColumn) column).setLength(length));
        }

        public void setNullableAndDefault(boolean nullable, String defaultValue) {
            actions.add(() -> column.setNullableAndDefault(nullable, defaultValue));
        }

        public void setCelestaDocLexem(String celestaDoc) {
            actions.add(() -> column.setCelestaDocLexem(celestaDoc));
        }
    }

    public final class ForeignKeyBuilder {
        private ForeignKey foreignKey;

        private ForeignKeyBuilder() {
        }

        public void addColumn(String columnName) {
            actions.add(() -> foreignKey.addColumn(columnName));
        }

        public void setConstraintName(String constraintName) {
            actions.add(() -> foreignKey.setConstraintName(constraintName));
        }

        public void setReferencedTable(String grain, String table) {
            actions.add(() -> foreignKey.setReferencedTable(grain, table));
        }

        public void addReferencedColumn(String columnName) {
            actions.add(() -> foreignKey.addReferencedColumn(columnName));
        }

        public void finalizeReference() {
            actions.add(() -> foreignKey.finalizeReference());
        }

        public void setUpdateRule(FKRule updateBehaviour) {
            actions.add(() -> foreignKey.setUpdateRule(updateBehaviour));
        }

        public void setDeleteRule(FKRule deleteBehaviour) {
            actions.add(() -> foreignKey.setDeleteRule(deleteBehaviour));
        }
    }

}

package ru.curs.celesta.score;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Foreign key class.
 */
public final class ForeignKey {

    private static final Logger LOGGER = LoggerFactory.getLogger(ForeignKey.class);

    private final BasicTable parentTable;
    private BasicTable referencedTable;
    private FKRule deleteRule = FKRule.NO_ACTION;
    private FKRule updateRule = FKRule.NO_ACTION;
    private String constraintName;

    private final NamedElementHolder<Column<?>> columns = new NamedElementHolder<Column<?>>() {
        @Override
        protected String getErrorMsg(String name) {
            return String
                    .format("Column '%s' defined more than once in foreign key for table '%s'.",
                            name, parentTable.getName());
        }
    };

    private final List<Column<?>> referencedColumns = new LinkedList<>();

    ForeignKey(BasicTable parentTable) {
        if (parentTable == null) {
            throw new IllegalArgumentException();
        }
        this.parentTable = parentTable;
    }

    public ForeignKey(BasicTable parentTable, BasicTable referencedTable,
            String[] columnNames) throws ParseException {
        this(parentTable);
        for (String n : columnNames) {
            addColumn(n);
        }
        setReferencedTable(referencedTable.getGrain().getName(),
                referencedTable.getName());
    }

    /**
     * Sets rule for deletion.
     *
     * @param deleteBehaviour  rule for deletion.
     * @throws ParseException  When trying to modify the system grain.
     */
    public void setDeleteRule(FKRule deleteBehaviour) throws ParseException {
        if (deleteBehaviour == null) {
            throw new IllegalArgumentException();
        }
        if (deleteBehaviour == FKRule.SET_NULL) {
            checkNullable();
        }
        parentTable.getGrain().modify();
        this.deleteRule = deleteBehaviour;
    }

    /**
     * Sets rule for update.
     *
     * @param updateBehaviour  rule for update.
     * @throws ParseException  When trying to modify the system grain.
     */
    public void setUpdateRule(FKRule updateBehaviour) throws ParseException {
        if (updateBehaviour == null) {
            throw new IllegalArgumentException();
        }
        if (updateBehaviour == FKRule.SET_NULL) {
            checkNullable();
        }
        parentTable.getGrain().modify();
        this.updateRule = updateBehaviour;
    }

    private void checkNullable() throws ParseException {
        for (Column<?> c : columns) {
            if (!c.isNullable()) {
                throw new ParseException(String.format("Error while "
                        + "creating FK for table '%s': column '%s' is not "
                        + "nullable and therefore 'SET NULL' behaviour cannot "
                        + "be applied.", parentTable.getName(), c.getName()));
            }
        }
    }

    /**
     * Unmodified list of columns for the foreign key.
     *
     * @return
     */
    public Map<String, Column<?>> getColumns() {
        return columns.getElements();
    }

    /**
     * Table that the foreign key is part of.
     *
     * @return
     */
    public BasicTable getParentTable() {
        return parentTable;
    }

    /**
     * Table that is being referenced by the foreign key.
     *
     * @return
     */
    public BasicTable getReferencedTable() {
        return referencedTable;
    }

    /**
     * Returns rule for deletion.
     *
     * @return
     */
    public FKRule getDeleteRule() {
        return deleteRule;
    }

    /**
     * Returns rule for update.
     *
     * @return
     */
    public FKRule getUpdateRule() {
        return updateRule;
    }

    /**
     * Adds a column. The column must belong to the parent table.
     *
     * @param columnName  column name
     * @return
     * @throws ParseException  in case if the column is not found
     */
    void addColumn(String columnName) throws ParseException {
        columnName = getParentTable().getGrain().getScore().getIdentifierParser().parse(columnName);
        Column<?> c = parentTable.getColumns().get(columnName);
        if (c == null) {
            throw new ParseException(
                    String.format(
                            "Error while creating FK: no column '%s' defined in table '%s'.",
                            columnName, parentTable.getName()));
        }
        columns.addElement(c);
    }

    /**
     * Adds table that is being referenced, and finalizes the creation of
     * the primary key, adding it to the parent table.
     *
     * @param grain  grain name
     * @param table  table name
     * @throws ParseException  in case if there's already a key with the same set of
     *                         fields (not necessarily referencing the same table) in
     *                         the table.
     */
    void setReferencedTable(String grain, String table) throws ParseException {
        table = getParentTable().getGrain().getScore().getIdentifierParser().parse(table);
        // Извлечение гранулы по имени.
        Grain gm;
        if ("".equals(grain) || parentTable.getGrain().getName().equals(grain)) {
            gm = parentTable.getGrain();
        } else {
            AbstractScore score = parentTable.getGrain().getScore();
            gm = score.getGrain(grain);

            if (gm.isModified()) {
                //TODO:Костыль, используем как флаг того, что гранула начала парситься - must be removed
                score.parseGrain(grain);
            }

            if (!gm.isParsingComplete()) {
                throw new ParseException(
                        String.format(
                                "Error creating foreign key '%s'-->'%s.%s': "
                                        + "due to previous parsing errors or "
                                        + "cycle reference involving grains '%s' and '%s'.",
                                parentTable.getName(), grain, table,
                                parentTable.getGrain().getName(), grain));
            }
        }

        // Извлечение таблицы по имени.
        BasicTable t = gm.getElement(table, BasicTable.class);
        referencedTable = t;

        // Проверка того факта, что поля ключа совпадают по типу
        // с полями первичного ключа таблицы, на которую ссылка

        Map<String, Column<?>> refpk = referencedTable.getPrimaryKey();
        if (columns.size() != refpk.size()) {
            throw new ParseException(
                    String.format(
                            "Error creating foreign key for table %s: it has different size with PK of table '%s'",
                            parentTable.getName(), referencedTable.getName()));
        }
        Iterator<Column<?>> i = referencedTable.getPrimaryKey().values()
                .iterator();
        for (Column<?> c : columns) {
            Column<?> c2 = i.next();
            if (c.getClass() != c2.getClass()) {
                throw new ParseException(
                        String.format(
                                "Error creating foreign key for table %s: its field "
                                        + "types do not coincide with field types of PK of table '%s'",
                                parentTable.getName(),
                                referencedTable.getName()));
            }
            if (c2 instanceof StringColumn) {
                if (((StringColumn) c2).getLength() != ((StringColumn) c)
                        .getLength()) {
                    throw new ParseException(
                            String.format(
                                    "Error creating foreign key for table %s: its string "
                                            + "field length do not coincide with field length of PK of table '%s'",
                                    parentTable.getName(),
                                    referencedTable.getName()));
                }
            }
        }

        // Добавление ключа к родительской таблице (с проверкой того факта, что
        // ключа с таким же набором полей не существует).
        parentTable.addFK(this);

    }

    @Override
    public int hashCode() {
        int result = 0;
        for (Column<?> c : columns) {
            result ^= c.getName().hashCode();
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ForeignKey) {
            ForeignKey fk = (ForeignKey) obj;
            if (columns.size() == fk.columns.size()) {
                Iterator<Column<?>> i = fk.columns.iterator();
                for (Column<?> c : columns) {
                    Column<?> c2 = i.next();
                    if (!c.getName().equals(c2.getName())) {
                        return false;
                    }
                }
                return true;
            } else {
                return false;
            }
        } else {
            return super.equals(obj);
        }
    }

    /**
     * Adds a column that is being referenced. A list of such columns is not
     * stored in the Foreign Key, for it is necessary to know the table name and
     * the primary key of table (references to UNIQUE combinations are not used
     * because the support of UNIQUE combinations is missing). The mechanism is
     * needed for control of text reference correctness.
     *
     * @param columnName  column name
     * @throws ParseException  if the column is not present in the table that is
     *                         being referenced
     */
    void addReferencedColumn(String columnName) throws ParseException {
        // Запускать этот метод можно только после простановки таблицы, на
        // которую ссылаемся.
        if (referencedTable == null) {
            throw new IllegalStateException();
        }
        columnName = getParentTable().getGrain().getScore().getIdentifierParser().parse(columnName);
        Column<?> c = referencedTable.getColumns().get(columnName);
        if (c == null) {
            throw new ParseException(
                    String.format(
                            "Error creating foreign key for table '%s': column '%s' is not defined in table '%s'",
                            parentTable.getName(), columnName,
                            referencedTable.getName()

                    ));
        }
        referencedColumns.add(c);

    }

    /**
     * Finalizes the list of fields that is being referenced by the FK. For ease of
     * testing and to save memory, the internal list of references is garbage
     * collected right after the finalization. It is neither stored or available
     * anywhere. Its sole role is to check the text correctness.
     *
     * @throws ParseException  if the set of fields doesn't correspond to the one
     *                         of the primary key.
     */
    void finalizeReference() throws ParseException {

        if (referencedTable == null) {
            throw new IllegalStateException();
        }
        Map<String, Column<?>> pk = referencedTable.getPrimaryKey();
        int size = referencedColumns.size();
        if (pk.size() != size) {
            referencedColumns.clear();
            throw new ParseException(String.format(
                    "Error creating foreign key for table '%s': primary key "
                            + "length in table '%s' is %d, but the number of "
                            + "reference fields is %d.", parentTable.getName(),
                    referencedTable.getName(), pk.size(), size));
        }
        Iterator<Column<?>> i = pk.values().iterator();
        for (Column<?> c : referencedColumns) {
            Column<?> c2 = i.next();
            if (!c.getName().equals(c2.getName())) {
                referencedColumns.clear();
                throw new ParseException(String.format(
                        "Error creating foreign key for table '%s': expected primary key "
                                + "field '%s'.'%s', but was '%s'.",
                        parentTable.getName(), referencedTable.getName(),
                        c2.getName(), c.getName()));
            }
        }
        referencedColumns.clear();
    }

    /**
     * Returns the name of FK constraint (or generates it if it's not provided).
     *
     * @return
     */
    public String getConstraintName() {
        if (constraintName != null) {
            return constraintName;
        }

        String result = String.format("fk_%s_%s_%s_%s_%s",
                parentTable.getGrain().getName(), parentTable.getName(),
                referencedTable.getGrain().getName(), referencedTable.getName(),
                columns.getElements().keySet().iterator().next());

        result = NamedElement.limitName(result);
        LOGGER.trace("{}", result);

        return result;
    }

    /**
     * Sets name for FK constraint.
     *
     * @param constraintName  new name of constraint
     * @throws ParseException  incorrect name of constraint
     */
    public void setConstraintName(String constraintName) throws ParseException {
        if (constraintName != null) {
            parentTable.getGrain().getScore().getIdentifierParser().parse(constraintName);
        }
        this.constraintName = constraintName;
    }

    /**
     * Deletes the foreign key.
     *
     * @throws ParseException  When trying to modify the system grain.
     */
    public void delete() throws ParseException {
        parentTable.removeFK(this);
    }

}

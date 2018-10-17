package ru.curs.celesta.score;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.DBType;

/**
 * Гранула.
 */
public final class Grain extends NamedElement {

    private final AbstractScore score;

    private VersionString version = VersionString.DEFAULT;

    private int length;

    private int checksum;

    private int dependencyOrder;

    private boolean parsingComplete = false;

    private boolean modified = true;

    private boolean isAutoupdate = true;

    private boolean isLock = false;

    private Set<GrainPart> grainParts = new LinkedHashSet<>();

    private final Map<Class<? extends GrainElement>, NamedElementHolder<? extends GrainElement>> grainElements
            = new HashMap<>();

    private final NamedElementHolder<Index> indices = new NamedElementHolder<Index>() {
        @Override
        protected String getErrorMsg(String name) {
            return String.format("Index '%s' defined more than once in a grain.", name);
        }
    };

    private final Set<String> constraintNames = new HashSet<>();

    public Grain(AbstractScore score, String name) throws ParseException {
        super(name, score.getIdentifierParser());
        if (name.indexOf("_") >= 0) {
            throw new ParseException("Invalid grain name '" + name + "'. No underscores are allowed for grain names.");
        }
        this.score = score;
        score.addGrain(this);

        //TODO: Что-то с этим надо сделать grainPath = new File(String.format("%s%s%s", score.getDefaultGrainPath(), File.separator, name));
    }

    @SuppressWarnings("unchecked")
    private <T extends GrainElement> NamedElementHolder<T> getElementsHolder(Class<T> cls) {
        return (NamedElementHolder<T>) grainElements.computeIfAbsent(cls, c -> new NamedElementHolder<T>() {
            @Override
            protected String getErrorMsg(String name) {
                return String.format("%s '%s' defined more than once in a grain.", c.getSimpleName(), name);
            }
        });
    }

    /**
     * Добавляет элемент в гранулу.
     *
     * @param element Новый элемент гранулы.
     * @throws ParseException В случае, если элемент с таким именем уже существует.
     */
    @SuppressWarnings("unchecked")
    <T extends GrainElement> void addElement(T element) throws ParseException {
        if (element.getGrain() != this) {
            throw new IllegalArgumentException();
        }

        Optional<String> typeNameOfElementWithSameName = grainElements.entrySet().stream()
                // Не рассматриваем тот же тип (у его холдера своя проверка)
                .filter(entry -> !entry.getKey().equals(element.getClass()))
                // Сводим все Map'ы в одну
                .map(entry -> (Set<? extends Map.Entry<String, ? extends GrainElement>>) entry.getValue()
                        .getElements().entrySet())
                .flatMap(entrySet -> entrySet.stream())
                // Ищем совпадения по имени
                .filter(entry -> entry.getKey().equals(element.getName())).findAny()
                .map(entry -> entry.getValue().getClass().getSimpleName());
        if (typeNameOfElementWithSameName.isPresent()) {
            throw new ParseException(
                    String.format("Cannot create grain element '%s', a %s with the same name already exists in grain '%s'.",
                            element.getName(), typeNameOfElementWithSameName.get(), getName()));
        }

        modify();
        getElementsHolder((Class<T>) element.getClass()).addElement(element);
    }

    /**
     * Возвращает набор элементов указанного типа, определённый в грануле.
     *
     * @param classOfElement Класс элементов из набора
     */
    public <T extends GrainElement> Map<String, T> getElements(Class<T> classOfElement) {
        return getElementsHolder(classOfElement).getElements();
    }

    /**
     * Возвращает набор элементов указанного типа, определённый в грануле.
     *
     * @param classOfElement Класс элементов из набора
     */
    private <T extends GrainElement> List<T> getElements(Class<T> classOfElement, GrainPart gp) {
        return getElements(classOfElement).values().stream()
                .filter(t -> gp == t.getGrainPart()).collect(Collectors.toList());
    }

    /**
     * Возвращает набор индексов, определённых в грануле.
     */
    public Map<String, Index> getIndices() {
        return indices.getElements();
    }

    /**
     * Возвращает набор материализованных представлений, определенных в грануле.
     */
    public Map<String, MaterializedView> getMaterializedViews() {
        return getElementsHolder(MaterializedView.class).getElements();
    }

    /**
     * Возвращает набор таблиц, определённый в грануле.
     */
    public Map<String, Table> getTables() {
        return getElementsHolder(Table.class).getElements();
    }

    /**
     * Возвращает набор представлений, определённый в грануле.
     */
    public Map<String, View> getViews() {
        return getElementsHolder(View.class).getElements();
    }

    /**
     * Возвращает элемент по его имени и классу, либо исключение с сообщением о
     * том, что элемент не найден.
     *
     * @param name           Имя
     * @param classOfElement Класс элемента
     * @throws ParseException Если элемент с таким именем и классом не найден в грануле.
     */
    public <T extends GrainElement> T getElement(String name, Class<T> classOfElement) throws ParseException {
        T result = getElementsHolder(classOfElement).get(name);
        if (result == null) {
            throw new ParseException(
                    String.format("%s '%s' not found in grain '%s'", classOfElement.getSimpleName(), name, getName()));
        }

        return result;
    }

    /**
     * Добавляет индекс.
     *
     * @param index Новый индекс гранулы.
     * @throws ParseException В случае, если индекс с таким именем уже существует.
     */
    public void addIndex(Index index) throws ParseException {
        if (index.getGrain() != this) {
            throw new IllegalArgumentException();
        }
        modify();
        indices.addElement(index);
    }

    synchronized void removeIndex(Index index) throws ParseException {
        modify();
        indices.remove(index);
        index.getTable().removeIndex(index);
    }

    synchronized <T extends DataGrainElement> void removeElement(T element) throws ParseException {
        if (element instanceof Table) {
            removeTable((Table) element);
        } else {
            modify();
            getElementsHolder(element.getClass()).remove(element);
        }
    }

    private synchronized void removeTable(Table table) throws ParseException {
        // Проверяем, не системную ли таблицу хотим удалить
        modify();

        // Удаляются все индексы на данной таблице
        List<Index> indToDelete = new LinkedList<>();
        for (Index ind : indices) {
            if (ind.getTable() == table) {
                indToDelete.add(ind);
            }
        }

        // Удаляются все внешние ключи, ссылающиеся на данную таблицу
        List<ForeignKey> fkToDelete = new LinkedList<>();
        for (Grain g : score.getGrains().values()) {
            for (Table t : g.getElements(Table.class).values()) {
                for (ForeignKey fk : t.getForeignKeys()) {
                    if (fk.getReferencedTable() == table) {
                        fkToDelete.add(fk);
                    }
                }
            }
        }

        for (Index ind : indToDelete) {
            ind.delete();
        }
        for (ForeignKey fk : fkToDelete) {
            fk.delete();
        }

        // Удаляется сама таблица
        getElementsHolder(Table.class).remove(table);
    }

    /**
     * Returns model that the grain belongs to.
     */
    public AbstractScore getScore() {
        return score;
    }

    /**
     * Value {@code false} indicates that grain was created with option WITH NO AUTOUPDATE,
     * and won't be updated. Default value is {@code true}.
     */
    public boolean isAutoupdate() {
        return isAutoupdate;
    }

    /**
     * Sets autoupdate option. Default value is {@code true}.
     * @param isAutoupdate
     */
    public void setAutoupdate(boolean isAutoupdate) {
        this.isAutoupdate = isAutoupdate;
    }

    /**
     * Returns the grain version.
     */
    public VersionString getVersion() {
        return version;
    }

    /**
     * Sets the grain version.
     *
     * @param version  Quoted-string. In course of processing single and double quotes are removed.
     * @throws ParseException  in case if format of quoted string is incorrect.
     */
    public void setVersion(String version) throws ParseException {
        modify();
        this.version = new VersionString(StringColumn.unquoteString(version));
    }

    /**
     * Sets the grain to locked state which prevents it from being updated in database. Default is {@code false}.
     *
     * @param isLock
     */
    public void setLock(boolean isLock) {
        this.isLock = isLock;
    }

    /**
     * Returns locked state. If {@code true} the grain is not updated in database. Default is {@code false}.
     * @return
     */
    public boolean isLock() {
        return this.isLock;
    }

    /**
     * Returns length of the script file that the grain was created from.
     */
    public int getLength() {
        return length;
    }

    void setLength(int length) {
        this.length = length;
    }

    /**
     * Returns checksum of the script file that the grain was created from.
     * Coincidence of version, length and checksum is considered to be a sufficient solution for
     * skipping the reading and update of the database structure.
     */
    public int getChecksum() {
        return checksum;
    }

    void setChecksum(int checksum) {
        this.checksum = checksum;
    }

    /**
     * Adding of constraint name (for checking if it is unique).
     *
     * @param name  Constraint name.
     * @throws ParseException  In case if a constraint with the same name has already been defined.
     */
    void addConstraintName(String name) throws ParseException {
        name = getScore().getIdentifierParser().parse(name);
        if (constraintNames.contains(name)) {
            throw new ParseException(String.format("Constraint '%s' is defined more than once in a grain.", name));
        }
        constraintNames.add(name);
    }

    /**
     * Indicates that the grain parsing from file is completed.
     */
    public boolean isParsingComplete() {
        return parsingComplete;
    }

    /**
     * If a grain has a higher number than the other grain then it means that it can depend from the first one.
     */
    public int getDependencyOrder() {
        return dependencyOrder;
    }

    /**
     * Indicates that the grain parsing is completed. A system method.
     * 
     * @throws ParseException
     */
    public void finalizeParsing() throws ParseException {

        for (String tableName: getElements(Table.class).keySet()) {
            String sequenceName = tableName + "_seq";
            SequenceElement se = getElementsHolder(SequenceElement.class).get(sequenceName);
            if (se != null) {
                throw new ParseException(
                        String.format(
                                "Identifier %s can't be used for the naming of sequence as  it'is reserved by Celesta.",
                                sequenceName
                        )
                );
            }
        }

        parsingComplete = true;
        modified = false;
        dependencyOrder = score.nextOrderCounter();
    }

    /**
     * Returns a flag of grain modification ({@code true} if parts of grain were modified in the runtime).
     */
    public boolean isModified() {
        return modified;
    }

    void modify() throws ParseException {
        if (getScore().getSysSchemaName().equals(getName()) && parsingComplete) {
            throw new ParseException("You cannot modify system grain.");
        }
        modified = true;
    }


    public void save(PrintWriter bw, GrainPart gp) throws IOException {
        writeCelestaDoc(this, bw);
        bw.printf("CREATE SCHEMA %s VERSION '%s'", getName(), getVersion().toString());
        if (!isAutoupdate) {
            bw.printf(" WITH NO AUTOUPDATE");
        }
        bw.printf(";%n");
        bw.println();

        bw.println("-- *** TABLES ***");
        List<Table> tables = getElements(Table.class, gp);
        for (Table t : tables) {
            t.save(bw);
        }

        bw.println("-- *** FOREIGN KEYS ***");
        for (Table t : tables) {
            for (ForeignKey fk : t.getForeignKeys()) {
                fk.save(bw);
            }
        }

        bw.println("-- *** INDICES ***");
        List<Index> indices = getElements(Index.class, gp);
        for (Index i : indices) {
            i.save(bw);
        }

        bw.println("-- *** VIEWS ***");
        List<View> views = getElements(View.class, gp);
        for (View v : views) {
            v.save(bw);
        }

        bw.println("-- *** MATERIALIZED VIEWS ***");
        List<MaterializedView> materializedViews = getElements(MaterializedView.class, gp);
        for (MaterializedView mv : materializedViews) {
            mv.save(bw);
        }

        bw.println("-- *** PARAMETERIZED VIEWS ***");
        List<ParameterizedView> parameterizedViews = getElements(ParameterizedView.class, gp);
        for (ParameterizedView pv : parameterizedViews) {
            pv.save(bw);
        }
    }

    /**
     * Saves grain back to source files.
     *
     * @ io error
     */
    void save()  {
        // Сохранять неизменённую гранулу нет смысла.
        if (!modified) {
            return;
        }

        for (GrainPart gp : this.grainParts) {

            File sourceFile = gp.getSourceFile();

            if (!sourceFile.exists()) {
                sourceFile.getParentFile().mkdirs();
            }

            try (
                    PrintWriter bw = new PrintWriter(
                            new OutputStreamWriter(
                                    new FileOutputStream(sourceFile), StandardCharsets.UTF_8))
            ) {
                save(bw, gp);
            } catch (IOException e) {
                throw new CelestaException("Cannot save '%s' grain script: %s", getName(), e.getMessage());
            }
        }
    }

    /**
     * Returns a view by its name or an exception with a message that the view was not found.
     *
     * @param name  View name
     * @throws ParseException  If view with that name was not found in the grain.
     */

    public View getView(String name) throws ParseException {
        return getElement(name, View.class);
    }

    static boolean writeCelestaDoc(NamedElement e, PrintWriter bw) {
        String doc = e.getCelestaDoc();
        if (doc == null) {
            return false;
        } else {
            bw.printf("/**%s*/%n", doc);
            return true;
        }
    }

    /**
     * Returns a table by its name or an exception with a message that the table was not found.
     *
     * @param name  Table name
     * @throws ParseException  If table with that name was not found in the grain.
     */
    public Table getTable(String name) throws ParseException {
        return getElement(name, Table.class);
    }

    private static final Pattern NATIVE_SQL = Pattern.compile("--\\{\\{(.*)--}}", Pattern.DOTALL);

    private final Map<DBType, List<NativeSqlElement>> beforeSql = new HashMap<>();
    private final Map<DBType, List<NativeSqlElement>> afterSql = new HashMap<>();

    void addNativeSql(String sql, boolean isBefore, DBType dbType, GrainPart grainPart) throws ParseException {
        Matcher m = NATIVE_SQL.matcher(sql);
        if (!m.matches()) {
            throw new ParseException("Native sql should match pattern --{{...--}}, was " + sql);
        }

        final List<NativeSqlElement> sqlList;

        if (isBefore) {
            sqlList = beforeSql.computeIfAbsent(dbType, (dbTypeVar) -> new ArrayList<>());
        } else {
            sqlList = afterSql.computeIfAbsent(dbType, (dbTypeVar) -> new ArrayList<>());
        }

        sqlList.add(
                new NativeSqlElement(grainPart, m.group(1))
        );
    }

    public List<NativeSqlElement> getBeforeSqlList(DBType dbType) {
        return beforeSql.getOrDefault(dbType, Collections.emptyList());
    }

    public List<NativeSqlElement> getAfterSqlList(DBType dbType) {
        return afterSql.getOrDefault(dbType, Collections.emptyList());
    }

    public Set<GrainPart> getGrainParts() {
        return grainParts;
    }

}

package ru.curs.celesta.score;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
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
		if (score == null)
			throw new IllegalArgumentException();
		if (name.indexOf("_") >= 0)
			throw new ParseException("Invalid grain name '" + name + "'. No underscores are allowed for grain names.");
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
	 * Возвращает набор материализованных представлений, определенных в грануле
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
        if (result == null)
            throw new ParseException(
                    String.format("%s '%s' not found in grain '%s'", classOfElement.getSimpleName(), name, getName()));
        return result;
    }

    /**
     * Добавляет индекс.
     *
     * @param index Новый индекс гранулы.
     * @throws ParseException В случае, если индекс с таким именем уже существует.
     */
    public void addIndex(Index index) throws ParseException {
        if (index.getGrain() != this)
            throw new IllegalArgumentException();
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
		for (Index ind : indices)
			if (ind.getTable() == table)
				indToDelete.add(ind);

		// Удаляются все внешние ключи, ссылающиеся на данную таблицу
		List<ForeignKey> fkToDelete = new LinkedList<>();
		for (Grain g : score.getGrains().values())
			for (Table t : g.getElements(Table.class).values())
				for (ForeignKey fk : t.getForeignKeys())
					if (fk.getReferencedTable() == table)
						fkToDelete.add(fk);

		for (Index ind : indToDelete)
			ind.delete();
		for (ForeignKey fk : fkToDelete)
			fk.delete();

		// Удаляется сама таблица
		getElementsHolder(Table.class).remove(table);
	}

	/**
	 * Возвращает модель, к которой принадлежит гранула.
	 */
	public AbstractScore getScore() {
		return score;
	}

	/**
	 * Возвращает номер версии гранулы.
	 */
	public VersionString getVersion() {
		return version;
	}

	/**
	 * Возвращает длину файла-скрипта, на основе которого создана гранула.
	 */
	public int getLength() {
		return length;
	}

	void setLength(int length) {
		this.length = length;
	}

	/**
	 * Возвращает контрольную сумму файла-скрипта, на основе которого создана
	 * гранула. Совпадение версии, длины и контрольной суммы считается
	 * достаточным условием для того, чтобы не заниматься чтением и обновлением
	 * структуры базы данных.
	 */
	public int getChecksum() {
		return checksum;
	}

	void setChecksum(int checksum) {
		this.checksum = checksum;
	}

    /**
     * Устанавливает версию гранулы.
     *
     * @param version Quoted-string. В процессе установки обрамляющие и двойные
     *                кавычки удаляются.
     * @throws ParseException в случае, если имеется неверный формат quoted string.
     */
    public void setVersion(String version) throws ParseException {
        modify();
        this.version = new VersionString(StringColumn.unquoteString(version));
    }

    /**
     * Добавление имени ограничения (для проверерки, что оно уникальное).
     *
     * @param name Имя ограничения.
     * @throws ParseException В случае, если ограничение с таким именем уже определено.
     */
    void addConstraintName(String name) throws ParseException {
    	name = getScore().getIdentifierParser().parse(name);
        if (constraintNames.contains(name))
            throw new ParseException(String.format("Constraint '%s' is defined more than once in a grain.", name));
        constraintNames.add(name);
    }

	/**
	 * Указывает на то, что разбор гранулы из файла завершён.
	 */
	public boolean isParsingComplete() {
		return parsingComplete;
	}

	/**
	 * Если одна гранула имеет номер больший, чем другая, то значит, что она
	 * может зависеть от первой.
	 */
	public int getDependencyOrder() {
		return dependencyOrder;
	}

	/**
	 * Указывает на то, что разбор гранулы завершен. Системный метод.
	 */
	public void finalizeParsing() throws ParseException {

		for (String tableName: getElements(Table.class).keySet()) {
			String sequenceName = tableName + "_seq";
			SequenceElement se = getElementsHolder(SequenceElement.class).get(sequenceName);
			if (se != null)
				throw new ParseException(
						String.format(
								"Identifier %s can't be used for the naming of sequence as  it'is reserved by Celesta.",
								sequenceName
						)
				);
		}

        parsingComplete = true;
        modified = false;
        dependencyOrder = score.nextOrderCounter();
    }

	/**
	 * Возвращает признак модификации гранулы (true, если составляющие части
	 * гранулы были модифицированы в runtime).
	 */
	public boolean isModified() {
		return modified;
	}

	void modify() throws ParseException {
		if (getScore().getSysSchemaName().equals(getName()) && parsingComplete)
			throw new ParseException("You cannot modify system grain.");
		modified = true;
	}


    public void save(PrintWriter bw, GrainPart gp) throws IOException {
        writeCelestaDoc(this, bw);
        bw.printf("CREATE SCHEMA %s VERSION '%s';%n", getName(), getVersion().toString());
        bw.println();

        bw.println("-- *** TABLES ***");
        List<Table> tables = getElements(Table.class, gp);
        for (Table t : tables)
            t.save(bw);

        bw.println("-- *** FOREIGN KEYS ***");
        for (Table t : tables)
            for (ForeignKey fk : t.getForeignKeys())
                fk.save(bw);

        bw.println("-- *** INDICES ***");
		List<Index> indices = getElements(Index.class, gp);
        for (Index i : indices)
            i.save(bw);

        bw.println("-- *** VIEWS ***");
		List<View> views = getElements(View.class, gp);
        for (View v : views)
            v.save(bw);

        bw.println("-- *** MATERIALIZED VIEWS ***");
		List<MaterializedView> materializedViews = getElements(MaterializedView.class, gp);
        for (MaterializedView mv : materializedViews)
            mv.save(bw);

        bw.println("-- *** PARAMETERIZED VIEWS ***");
		List<ParameterizedView> parameterizedViews = getElements(ParameterizedView.class, gp);
        for (ParameterizedView pv : parameterizedViews)
            pv.save(bw);
    }

    /**
     * Saves grain back to source files
     *
     * @ io error
     */
    void save()  {
		// Сохранять неизменённую гранулу нет смысла.
		if (!modified)
			return;

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
     * Возвращает представление по его имени, либо исключение с сообщением о
     * том, что представление не найдено.
     *
     * @param name Имя
     * @throws ParseException Если таблица с таким именем не найдена в грануле.
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
     * Возвращает таблицу по её имени, либо исключение с сообщением о том, что
     * таблица не найдена.
     *
     * @param name Имя
     * @throws ParseException Если таблица с таким именем не найдена в грануле.
     */
    public Table getTable(String name) throws ParseException {
        return getElement(name, Table.class);
    }

	private static final Pattern NATIVE_SQL = Pattern.compile("--\\{\\{(.*)--}}", Pattern.DOTALL);

    private final Map<DBType, List<NativeSqlElement>> beforeSql = new HashMap<>();
    private final Map<DBType, List<NativeSqlElement>> afterSql = new HashMap<>();

    void addNativeSql(String sql, boolean isBefore, DBType dbType, GrainPart grainPart) throws ParseException {
		Matcher m = NATIVE_SQL.matcher(sql);
		if (!m.matches())
			throw new ParseException("Native sql should match pattern --{{...--}}, was " + sql);

    	final List<NativeSqlElement> sqlList;

    	if (isBefore)
			sqlList = beforeSql.computeIfAbsent(dbType, (dbTypeVar) -> new ArrayList<>());
    	else
			sqlList = afterSql.computeIfAbsent(dbType, (dbTypeVar) -> new ArrayList<>());

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

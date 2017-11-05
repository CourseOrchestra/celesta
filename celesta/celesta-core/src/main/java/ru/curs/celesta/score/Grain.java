package ru.curs.celesta.score;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import ru.curs.celesta.CelestaException;

/**
 * Гранула.
 */
public final class Grain extends NamedElement {

	/**
	 * Счётчик вызовов метода parsingComplete для заполнения свойства
	 * dependencyOrder.
	 */
	private static int orderCounter = 0;

	private final Score score;

	private VersionString version = VersionString.DEFAULT;

	private int length;

	private int checksum;

	private int dependencyOrder;

	private boolean parsingComplete = false;

	private boolean modified = true;

	private File grainPath;

	private final Map<Class<? extends DataGrainElement>, NamedElementHolder<? extends DataGrainElement>> grainElements = new HashMap<>();

	private final NamedElementHolder<Index> indices = new NamedElementHolder<Index>() {
		@Override
		protected String getErrorMsg(String name) {
			return String.format("Index '%s' defined more than once in a grain.", name);
		}
	};

	private final Set<String> constraintNames = new HashSet<>();

	public Grain(Score score, String name) throws ParseException {
		super(name);
		if (score == null)
			throw new IllegalArgumentException();
		if (name.indexOf("_") >= 0)
			throw new ParseException("Invalid grain name '" + name + "'. No underscores are allowed for grain names.");
		this.score = score;
		score.addGrain(this);

		grainPath = new File(String.format("%s%s%s", score.getDefaultGrainPath(), File.separator, name));
	}

	@SuppressWarnings("unchecked")
	private <T extends DataGrainElement> NamedElementHolder<T> getElementsHolder(Class<T> cls) {
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
	 * @param element
	 *            Новый элемент гранулы.
	 * @throws ParseException
	 *             В случае, если элемент с таким именем уже существует.
	 */
	@SuppressWarnings("unchecked")
	<T extends DataGrainElement> void addElement(T element) throws ParseException {
		if (element.getGrain() != this) {
			throw new IllegalArgumentException();
		}

		Optional<String> typeNameOfElementWithSameName = grainElements.entrySet().stream()
				// Не рассматриваем тот же тип (у его холдера своя проверка)
				.filter(entry -> !entry.getKey().equals(element.getClass()))
				// Сводим все Map'ы в одну
				.map(entry -> (Set<? extends Map.Entry<String, ? extends DataGrainElement>>) entry.getValue()
						.getElements().entrySet())
				.flatMap(entrySet -> entrySet.stream())
				// Ищем совпадения по имени
				.filter(entry -> entry.getKey().equals(element.getName())).findAny()
				.map(entry -> entry.getValue().getClass().getSimpleName());
		if (typeNameOfElementWithSameName.isPresent()) {
			throw new ParseException(
					String.format("Cannot create table '%s', a %s with the same name already exists in grain '%s'.",
							element.getName(), typeNameOfElementWithSameName.get(), getName()));
		}

		modify();
		getElementsHolder((Class<T>) element.getClass()).addElement(element);
	}

	/**
	 * Возвращает набор элементов указанного типа, определённый в грануле.
	 *
	 * @param classOfElement
	 *            Класс элементов из набора
	 */
	public <T extends DataGrainElement> Map<String, T> getElements(Class<T> classOfElement) {
		return getElementsHolder(classOfElement).getElements();
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
	 * @param name
	 *            Имя
	 * @param classOfElement
	 *            Класс элемента
	 * @throws ParseException
	 *             Если элемент с таким именем и классом не найден в грануле.
	 */
	public <T extends DataGrainElement> T getElement(String name, Class<T> classOfElement) throws ParseException {
		T result = getElementsHolder(classOfElement).get(name);
		if (result == null)
			throw new ParseException(
					String.format("%s '%s' not found in grain '%s'", classOfElement.getSimpleName(), name, getName()));
		return result;
	}

	/**
	 * Добавляет индекс.
	 *
	 * @param index
	 *            Новый индекс гранулы.
	 * @throws ParseException
	 *             В случае, если индекс с таким именем уже существует.
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
	public Score getScore() {
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
	 * @param version
	 *            Quoted-string. В процессе установки обрамляющие и двойные
	 *            кавычки удаляются.
	 * @throws ParseException
	 *             в случае, если имеется неверный формат quoted string.
	 */
	public void setVersion(String version) throws ParseException {
		modify();
		this.version = new VersionString(StringColumn.unquoteString(version));
	}

	/**
	 * Добавление имени ограничения (для проверерки, что оно уникальное).
	 *
	 * @param name
	 *            Имя ограничения.
	 * @throws ParseException
	 *             В случае, если ограничение с таким именем уже определено.
	 */
	void addConstraintName(String name) throws ParseException {
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
	public void completeParsing() {
		parsingComplete = true;
		modified = false;
		orderCounter++;
		dependencyOrder = orderCounter;
	}

	/**
	 * Возвращает путь к грануле.
	 */
	public File getGrainPath() {
		return grainPath;
	}

	void setGrainPath(File grainPath) {
		if (!grainPath.isDirectory())
			throw new IllegalArgumentException();
		this.grainPath = grainPath;
	}

	/**
	 * Возвращает признак модификации гранулы (true, если составляющие части
	 * гранулы были модифицированы в runtime).
	 */
	public boolean isModified() {
		return modified;
	}

	void modify() throws ParseException {
		if ("celesta".equals(getName()) && parsingComplete)
			throw new ParseException("You cannot modify system grain.");
		modified = true;
	}

	/**
	 * Сохраняет гранулу обратно в файл, расположенный в grainPath.
	 *
	 * @throws CelestaException
	 *             ошибка ввода-вывода
	 */
	void save() throws CelestaException {
		// Сохранять неизменённую гранулу нет смысла.
		if (!modified)
			return;
		if (!grainPath.exists())
			grainPath.mkdirs();
		File scriptFile = new File(String.format("%s%s_%s.sql", grainPath.getPath(), File.separator, getName()));
		try (
			BufferedWriter bw = new BufferedWriter(
					new OutputStreamWriter(
							new FileOutputStream(scriptFile), StandardCharsets.UTF_8))
		) {
				writeCelestaDoc(this, bw);
				bw.write("CREATE GRAIN ");
				bw.write(getName());
				bw.write(" VERSION '");
				bw.write(getVersion().toString());
				bw.write("';");
				bw.newLine();
				bw.newLine();
				bw.write("-- *** TABLES ***");
				bw.newLine();
				for (Table t : getElements(Table.class).values())
					t.save(bw);

				bw.write("-- *** FOREIGN KEYS ***");
				bw.newLine();
				int j = 1;
				for (Table t : getElements(Table.class).values())
					for (ForeignKey fk : t.getForeignKeys())
						fk.save(bw, j++);

				bw.write("-- *** INDICES ***");
				bw.newLine();
				for (Index i : getIndices().values())
					i.save(bw);

				bw.write("-- *** VIEWS ***");
				bw.newLine();
				for (View v : getElements(View.class).values())
					v.save(bw);

				bw.write("-- *** MATERIALIZED VIEWS ***");
				bw.newLine();
				for (MaterializedView mv : getElements(MaterializedView.class).values())
					mv.save(bw);

				bw.write("-- *** PARAMETERIZED VIEWS ***");
				bw.newLine();
				for (ParameterizedView pv : getElements(ParameterizedView.class).values())
					pv.save(bw);
		} catch (IOException e) {
			throw new CelestaException("Cannot save '%s' grain script: %s", getName(), e.getMessage());
		}
	}

	static boolean writeCelestaDoc(NamedElement e, BufferedWriter bw) throws IOException {
		String doc = e.getCelestaDoc();
		if (doc == null) {
			return false;
		} else {
			bw.write("/**");
			bw.write(doc);
			bw.write("*/");
			bw.newLine();
			return true;
		}
	}

	/**
	 * Возвращает представление по его имени, либо исключение с сообщением о
	 * том, что представление не найдено.
	 * 
	 * @param name
	 *            Имя
	 * @throws ParseException
	 *             Если таблица с таким именем не найдена в грануле.
	 */

	public View getView(String name) throws ParseException {
		return getElement(name, View.class);
	}

	/**
	 * Возвращает таблицу по её имени, либо исключение с сообщением о том, что
	 * таблица не найдена.
	 * 
	 * @param name
	 *            Имя
	 * @throws ParseException
	 *             Если таблица с таким именем не найдена в грануле.
	 */
	public Table getTable(String name) throws ParseException {
		return getElement(name, Table.class);
	}

}

package ru.curs.celesta.score;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

/**
 * Базовый класс для элементов гранулы (таблиц, индексов и представлений).
 */
public abstract class GrainElement extends NamedElement {

	/**
	 * Гранула, к которой относится данный элемент.
	 */
	private final Grain grain;

	public GrainElement(Grain g, String name) throws ParseException {
		super(name);
		if (g == null)
			throw new IllegalArgumentException();
		grain = g;
	}

	/**
	 * Возвращает гранулу, к которой относится элемент.
	 */
	public final Grain getGrain() {
		return grain;
	}

	/**
	 * Перечень столбцов с именами.
	 */
	public abstract Map<String, ? extends ColumnMeta> getColumns();

	/**
	 * Номер столбца в перечне столбцов.
	 * 
	 * @param name
	 *            Имя столбца.
	 */
	public abstract int getColumnIndex(String name);

	abstract void save(BufferedWriter bw) throws IOException;

	/**
	 * Возвращает Celesta-SQL представление объекта.
	 * 
	 * @throws IOException
	 *             ошибка ввода-вывода при сохранении.
	 */
	public String getCelestaSQL() throws IOException {
		StringWriter sw = new StringWriter();
		BufferedWriter bw = new BufferedWriter(sw);
		save(bw);
		bw.flush();
		return sw.toString();
	}
}

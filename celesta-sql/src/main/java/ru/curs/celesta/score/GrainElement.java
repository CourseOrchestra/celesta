package ru.curs.celesta.score;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

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

	abstract void save(PrintWriter bw) throws IOException;

	/**
	 * Возвращает Celesta-SQL представление объекта.
	 * 
	 * @throws IOException
	 *             ошибка ввода-вывода при сохранении.
	 */
	public String getCelestaSQL() throws IOException {
		StringWriter sw = new StringWriter();
		PrintWriter bw = new PrintWriter(sw);
		save(bw);
		bw.flush();
		return sw.toString();
	}
}

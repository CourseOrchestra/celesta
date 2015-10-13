package ru.curs.celesta.score;

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
}

package ru.curs.celesta;

/**
 * Именованный элемент метамодели (например, таблица или колонка), который
 * должен иметь уникальное имя-идентификатор.
 * 
 */
public abstract class NamedElement {

	private final String name;

	public NamedElement(String name) {
		// Не должно быть name==null, т. к. все методы написаны исходя из того,
		// что name != null.
		if (name == null)
			throw new IllegalArgumentException();
		this.name = name;
	}

	public final String getName() {
		return name;
	}

	@Override
	public final int hashCode() {
		return name.hashCode();
	}

	@Override
	public final boolean equals(Object obj) {
		return obj instanceof NamedElement ? name.equals(((NamedElement) obj)
				.getName()) : name.equals(obj);
	}
}

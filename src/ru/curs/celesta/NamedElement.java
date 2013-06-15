package ru.curs.celesta;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Именованный элемент метамодели (например, таблица или колонка), который
 * должен иметь уникальное имя-идентификатор.
 * 
 */
abstract class NamedElement {

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

/**
 * Перечень именованных элементов, который может содежать только по одному
 * элементу каждого имени. Обеспечивает уникальность имён и доступ по имени.
 * Например, это могут быть таблицы в грануле, поля в таблице, поля в ключе и т.
 * д.
 * 
 * @param <T>
 *            Тип именованных элементов в перечне.
 */
abstract class NamedElementHolder<T extends NamedElement> {
	private final Map<String, T> namespace = new LinkedHashMap<>();

	Map<String, T> getElements() {
		return Collections.unmodifiableMap(namespace);
	}

	abstract String getErrorMsg(String name);

	final void addElement(T element) throws ParseException {
		T oldValue = namespace.put(element.getName(), element);
		if (oldValue != null) {
			namespace.put(oldValue.getName(), oldValue);
			throw new ParseException(getErrorMsg(element.getName()));
		}
	}

	final T get(String name) {
		return namespace.get(name);
	}

	final boolean isEmpty() {
		return namespace.isEmpty();
	}
}
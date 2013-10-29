package ru.curs.celesta.score;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
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
 * Реализация интерфейса Collection выполнена только лишь с той целью, чтобы
 * утилита ObjectAid правильно выстраивала UML-диаграмму (воспринимая поле с
 * типом NamedElementHolder как мульти-ссылку).
 * 
 * @param <T>
 *            Тип именованных элементов в перечне.
 */
abstract class NamedElementHolder<T extends NamedElement> implements
		Collection<T> {
	private final Map<String, T> namespace = new LinkedHashMap<>();
	private final Map<String, T> namespaceReadOnly = Collections
			.unmodifiableMap(namespace);

	Map<String, T> getElements() {
		return namespaceReadOnly;
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

	@Override
	public final boolean isEmpty() {
		return namespace.isEmpty();
	}

	@Override
	public Iterator<T> iterator() {
		return namespace.values().iterator();
	}

	@Override
	public int size() {
		return namespace.size();
	}

	@Override
	public boolean contains(Object o) {
		return namespace.containsValue(o);
	}

	@Override
	public Object[] toArray() {
		return namespace.values().toArray();
	}

	@Override
	public <A> A[] toArray(A[] a) {
		return namespace.values().toArray(a);
	}

	@Override
	@Deprecated
	public boolean add(T e) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean remove(Object o) {
		if (o instanceof NamedElement) {
			NamedElement e = (NamedElement) o;
			return namespace.remove(e.getName()) != null;
		} else {
			return false;
		}
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clear() {
		namespace.clear();
	}

}
package ru.curs.celesta.score;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

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
public abstract class NamedElementHolder<T extends NamedElement> implements Collection<T> {
	private final LinkedHashMap<String, T> namespace = new LinkedHashMap<>();
	private final Map<String, T> namespaceReadOnly = Collections.unmodifiableMap(namespace);

	/**
	 * Возвращает копию словаря элементов только для чтения.
	 */
	public Map<String, T> getElements() {
		return namespaceReadOnly;
	}

	protected abstract String getErrorMsg(String name);

	/**
	 * Добавляет именованный элемент.
	 * 
	 * @param element
	 *            Добавляемый элемент.
	 * @throws ParseException
	 *             Если элемент с таким именем уже существует.
	 */
	public final void addElement(T element) throws ParseException {
		T oldValue = namespace.put(element.getName(), element);
		if (oldValue != null) {
			namespace.put(oldValue.getName(), oldValue);
			throw new ParseException(getErrorMsg(element.getName()));
		}
	}

	/**
	 * Возвращает элемент по имени.
	 * 
	 * @param name
	 *            Имя (идентификатор) элемента.
	 */
	public final T get(String name) {
		return namespace.get(name);
	}

	/**
	 * Возвращает индекс элемента по имени.
	 * 
	 * @param name
	 *            Имя (идентификатор) элемента.
	 */
	public int getIndex(String name) {
		int i = -1;
		for (String c : namespace.keySet()) {
			i++;
			if (c.equals(name))
				return i;
		}
		return i;
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
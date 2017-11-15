package ru.curs.lyra;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import ru.curs.celesta.CelestaException;

/**
 * An analogue of NamedElementHolder for LyraNamedElement.
 *
 * @param <T>
 */
public abstract class LyraNamedElementHolder<T extends LyraNamedElement> implements Collection<T>, Serializable {

	private static final long serialVersionUID = 1L;
	private final Map<String, T> namespace = new LinkedHashMap<>();
	private final transient Map<String, T> namespaceReadOnly = Collections.unmodifiableMap(namespace);

	/**
	 * The read-only copy of map.
	 */
	public Map<String, T> getElements() {
		return namespaceReadOnly;
	}

	protected abstract String getErrorMsg(String name);

	/**
	 * Adds a named element.
	 * 
	 * @param element
	 *            Element to be added.
	 * @throws CelestaException
	 *             If element already exists.
	 */
	public final void addElement(T element) throws CelestaException {
		T oldValue = namespace.put(element.getName(), element);
		if (oldValue != null) {
			namespace.put(oldValue.getName(), oldValue);
			throw new CelestaException(getErrorMsg(element.getName()));
		}
	}

	/**
	 * Gets element by its name.
	 * 
	 * @param name
	 *            Name of the element.
	 */
	public final T get(String name) {
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
		if (o instanceof LyraNamedElement) {
			LyraNamedElement e = (LyraNamedElement) o;
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
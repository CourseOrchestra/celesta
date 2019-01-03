package ru.curs.celesta.score;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A collection of named elements that may only contain one element per each name.
 * Provides uniqueness of names and access by name. For example these may be tables
 * in a grain, fields in a key etc.
 *
 * The implementation of interface Collection is done only for the purpose of
 * ObjectAid utility to correctly build a UML diagram (taking a field of
 * NamedElementHolder type as a multi-reference).
 * 
 *
 * @param <T>  type of named elements in the collection.
 */
public abstract class NamedElementHolder<T extends NamedElement> implements Collection<T> {
    private final LinkedHashMap<String, T> namespace = new LinkedHashMap<>();
    private final Map<String, T> namespaceReadOnly = Collections.unmodifiableMap(namespace);

    /**
     * Returns a copy of elements map only for reading.
     *
     * @return
     */
    public Map<String, T> getElements() {
        return namespaceReadOnly;
    }

    protected abstract String getErrorMsg(String name);

    /**
     * Adds a named element.
     *
     * @param element  element to add
     * @throws ParseException  if an element with the same name already exists.
     */
    public final void addElement(T element) throws ParseException {
        T oldValue = namespace.put(element.getName(), element);
        if (oldValue != null) {
            namespace.put(oldValue.getName(), oldValue);
            throw new ParseException(getErrorMsg(element.getName()));
        }
    }

    /**
     * Returns element by name.
     *
     * @param name  element name (identifier)
     * @return
     */
    public final T get(String name) {
        return namespace.get(name);
    }

    /**
     * Returns index of element by name.
     *
     * @param name  element name (identifier)
     * @return
     */
    public int getIndex(String name) {
        int i = -1;
        for (String c : namespace.keySet()) {
            i++;
            if (c.equals(name)) {
                return i;
            }
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

package ru.curs.celesta.dbutils;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class CursorIterator<T extends BasicCursor> implements Iterator<T> {

    private final T cursor;
    private final boolean hasResults;
    private boolean isRead = false;

    public CursorIterator(T cursor) {
        this.cursor = cursor;
        this.hasResults = cursor.tryFindSet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        if (!this.hasResults) {
            return false;
        }

        if (isRead) {
            this.isRead = false;
            return this.cursor.nextInSet();
        } else {
            return true;
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T next() {
        if (this.isRead) {
            if (!this.cursor.nextInSet()) {
                throw new NoSuchElementException();
            }
        } else {
            this.isRead = true;
        }

        return this.cursor;
    }

}

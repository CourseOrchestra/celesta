package ru.curs.celesta.dbutils;

import ru.curs.celesta.CelestaException;

import java.sql.SQLException;
import java.util.Iterator;

public class CursorIterator<T extends BasicCursor> implements Iterator<T> {

    private final T cursor;
    private final boolean hasResults;
    private boolean justCreated = true;

    public CursorIterator(T cursor) {
        this.cursor = cursor;
        this.hasResults = cursor.tryFindSet();
    }

    @Override
    public boolean hasNext() {
        if (!this.hasResults) {
            return false;
        }
        if (this.justCreated) {
            return true;
        }
        try {
            boolean result = this.cursor.cursor.next();
            this.cursor.cursor.previous();
            return result;
        } catch (SQLException e) {
            throw new CelestaException(e);
        }
    }

    @Override
    public T next() {
        if (!this.justCreated) {
            this.cursor.nextInSet();
        } else {
            this.justCreated = false;
        }
        return this.cursor;
    }
}

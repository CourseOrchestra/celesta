package ru.curs.celesta.dbutils;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.filter.In;
import ru.curs.celesta.dbutils.filter.value.FieldsLookup;
import ru.curs.celesta.dbutils.term.WhereTermsMaker;

import java.util.Objects;
import java.util.function.Function;

public final class InFilterHolder {

    private final BasicCursor cursor;
    private In inFilter;


    public InFilterHolder(BasicCursor cursor) {
        this.cursor = cursor;
    }

    final FieldsLookup setIn(BasicCursor otherCursor) {

        Runnable lookupChangeCallback = () -> {
            if (!cursor.isClosed()) {
                // recreate the data set
                cursor.closeSet();
            }
        };

        Function<FieldsLookup, Void> newLookupCallback = lookup -> {
            inFilter.addLookup(lookup, lookup.getOtherCursor().getQmaker());
            return null;
        };

        FieldsLookup fieldsLookup;

        if (cursor instanceof Cursor) {
            fieldsLookup = new FieldsLookup(
                    (Cursor) cursor, otherCursor, lookupChangeCallback, newLookupCallback);
        } else if (cursor instanceof ViewCursor) {
            fieldsLookup = new FieldsLookup(
                    (ViewCursor) cursor, otherCursor, lookupChangeCallback, newLookupCallback);
        } else {
            throw new CelestaException("Not supported cursor type: %s", cursor.getClass().getSimpleName());
        }

        WhereTermsMaker otherWhereTermMaker = otherCursor.getQmaker();
        inFilter = new In(fieldsLookup, otherWhereTermMaker);

        return fieldsLookup;
    }

    final In getIn() {
        return inFilter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof InFilterHolder)) return false;

        InFilterHolder other = (InFilterHolder) o;
        return Objects.equals(inFilter, other.inFilter);
    }
}

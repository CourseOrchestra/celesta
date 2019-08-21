package ru.curs.celesta.script;

import navigation.NavigationTableCursor;
import org.junit.jupiter.api.TestTemplate;
import ru.curs.celesta.CallContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ru.curs.celesta.CelestaException;

class TestNavigation implements ScriptTest {

    @TestTemplate
    void testSimpleNext(CallContext context) {
        NavigationTableCursor c = new NavigationTableCursor(context);
        _prepareTableForTest(c);

        c.orderBy(NavigationTableCursor.numb_COLUMN);
        c.first();
        assertEquals(1, c.getNumb().intValue());
        c.next();
        assertEquals(2, c.getNumb().intValue());
        c.next();
        assertEquals(3, c.getNumb().intValue());
        c.next();
        assertEquals(4, c.getNumb().intValue());
        c.next();
        assertEquals(5, c.getNumb().intValue());
        c.next();
        assertEquals(5, c.getNumb().intValue());


    }

    @TestTemplate
    void testSimplePrevious(CallContext context) {
        NavigationTableCursor c = new NavigationTableCursor(context);
        _prepareTableForTest(c);

        c.orderBy(NavigationTableCursor.numb_COLUMN);
        c.last();
        assertEquals(5, c.getNumb().intValue());
        c.previous();
        assertEquals(4, c.getNumb().intValue());
        c.previous();
        assertEquals(3, c.getNumb().intValue());
        c.previous();
        assertEquals(2, c.getNumb().intValue());
        c.previous();
        assertEquals(1, c.getNumb().intValue());
        c.previous();
        assertEquals(1, c.getNumb().intValue());


    }

    @TestTemplate
    void testNavigateWithOffset(CallContext context) {
        NavigationTableCursor c = new NavigationTableCursor(context);
        _prepareTableForTest(c);

        c.orderBy(NavigationTableCursor.numb_COLUMN);
        c.first();
        assertEquals(1, c.getNumb().intValue());

        c.navigate(">", 3);
        assertEquals(4, c.getNumb().intValue());

        c.navigate("<", 2);
        assertEquals(2, c.getNumb().intValue());

        assertFalse(c.navigate("<", 10));
        assertEquals(2, c.getNumb().intValue());

        assertFalse(c.navigate(">", 10));
        assertEquals(2, c.getNumb().intValue());

    }

    @TestTemplate
    void testCelestaExceptionWhenOffsetLessThanZero(CallContext context) {
        NavigationTableCursor c = new NavigationTableCursor(context);
        _prepareTableForTest(c);

        c.orderBy(NavigationTableCursor.numb_COLUMN);
        c.first();
        assertEquals(1, c.getNumb().intValue());

        assertThrows(CelestaException.class,
                () -> c.navigate(">", -1));


    }

    void _prepareTableForTest(NavigationTableCursor c) {
        c.deleteAll();
        _insert(c, 1);
        _insert(c, 2);
        _insert(c, 3);
        _insert(c, 4);
        _insert(c, 5);
        c.clear();


    }

    void _insert(NavigationTableCursor c, int numb) {
        c.clear();
        c.setNumb(numb);
        c.insert();
        c.clear();
    }
}

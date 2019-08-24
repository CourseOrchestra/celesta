package ru.curs.celesta.script;

import filters.*;
import org.junit.jupiter.api.TestTemplate;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.InFilterSupport;
import ru.curs.celesta.dbutils.filter.value.FieldsLookup;
import ru.curs.celesta.score.ParseException;

import java.sql.Timestamp;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


class TestFilters implements ScriptTest {

    @TestTemplate
    void testInFilterForTable(CallContext context) throws ParseException {
        AFilterCursor a = new AFilterCursor(context);
        BFilterCursor b = new BFilterCursor(context);
        _testInFilterForIndices(context, a, b);

    }

    @TestTemplate
    void testInFilterForView(CallContext context) throws ParseException {
        AFilterViewCursor a = new AFilterViewCursor(context);
        BFilterViewCursor b = new BFilterViewCursor(context);
        _testInFilterForIndices(context, a, b);


    }

    @TestTemplate
    void testInFilterForSimplePks(CallContext context) throws ParseException {
        CFilterCursor c = new CFilterCursor(context);
        DFilterCursor d = new DFilterCursor(context);

        c.deleteAll();
        d.deleteAll();

        c.setId(1);
        c.insert();
        c.clear();

        c.setId(2);
        c.insert();
        c.clear();

        c.setId(3);
        c.insert();
        c.clear();

        d.setId(1);
        d.insert();
        d.clear();

        d.setId(3);
        d.insert();
        d.clear();

        c.setIn(d).add("id", "id");
        assertEquals(2, c.count());
    }

    @TestTemplate
    void testInFilterForComplexPks(CallContext context) throws ParseException {
        EFilterCursor e = new EFilterCursor(context);
        FFilterCursor f = new FFilterCursor(context);

        e.deleteAll();
        f.deleteAll();

        e.setId(1);
        e.setNumber(1);
        e.setStr("A");
        e.insert();
        e.clear();

        e.setId(1);
        e.setNumber(1);
        e.setStr("B");
        e.insert();
        e.clear();

        e.setId(1);
        e.setNumber(3);
        e.setStr("B");
        e.insert();
        e.clear();

        f.setId(1);
        f.setNumb(1);
        f.insert();
        f.clear();

        e.setIn(f).add("id", "id").add("number", "numb");
        assertEquals(2, e.count());


    }

    @TestTemplate
    void testInFilterWithRangeInMainCursorForTable(CallContext context) throws ParseException {
        AFilterCursor a = new AFilterCursor(context);
        BFilterCursor b = new BFilterCursor(context);
        _testInFilterWithRangeInMainCursor(context, a, b);

    }

    @TestTemplate
    void testInFilterWithRangeInMainCursorForView(CallContext context) throws ParseException {
        AFilterViewCursor a = new AFilterViewCursor(context);
        BFilterViewCursor b = new BFilterViewCursor(context);
        _testInFilterWithRangeInMainCursor(context, a, b);

    }

    @TestTemplate
    void testInFilterWithRangeInOtherCursorBeforeSetInForTable(CallContext context) throws ParseException {
        AFilterCursor a = new AFilterCursor(context);
        BFilterCursor b = new BFilterCursor(context);
        _testInFilterWithRangeInOtherCursorBeforeSetIn(context, a, b);

    }

    @TestTemplate
    void testInFilterWithRangeInOtherCursorBeforeSetInForView(CallContext context) throws ParseException {
        AFilterViewCursor a = new AFilterViewCursor(context);
        BFilterViewCursor b = new BFilterViewCursor(context);
        _testInFilterWithRangeInOtherCursorBeforeSetIn(context, a, b);

    }

    @TestTemplate
    void testInFilterWithRangeInOtherCursorAfterSetInForTable(CallContext context) throws ParseException {
        AFilterCursor a = new AFilterCursor(context);
        BFilterCursor b = new BFilterCursor(context);
        _testInFilterWithRangeInOtherCursorAfterSetIn(context, a, b);

    }

    @TestTemplate
    void testInFilterWithRangeInOtherCursorAfterSetInForView(CallContext context) throws ParseException {
        AFilterViewCursor a = new AFilterViewCursor(context);
        BFilterViewCursor b = new BFilterViewCursor(context);
        _testInFilterWithRangeInOtherCursorAfterSetIn(context, a, b);

    }

    @TestTemplate
    void testInFilterWithAdditionalLookupForTable(CallContext context) throws ParseException {
        AFilterCursor a = new AFilterCursor(context);
        BFilterCursor b = new BFilterCursor(context);
        GFilterCursor g = new GFilterCursor(context);

        _testInFilterWithAdditionalLookup(context, a, b, g);

    }

    @TestTemplate
    void testInFilterWithAdditionalLookupForView(CallContext context) throws ParseException {
        AFilterViewCursor a = new AFilterViewCursor(context);
        BFilterViewCursor b = new BFilterViewCursor(context);
        GFilterViewCursor g = new GFilterViewCursor(context);
        _testInFilterWithAdditionalLookup(context, a, b, g);


    }

    @TestTemplate
    void testInFilterWhenTargetHasPkAndOtherHasPkWithNotSameOrderAndIndexWithSameOrder(CallContext context) throws ParseException {
        HFilterCursor h = new HFilterCursor(context);
        IFilterCursor i = new IFilterCursor(context);

        h.deleteAll();
        i.deleteAll();

        h.setId("H1");
        h.insert();
        h.clear();

        h.setId("H2");
        h.insert();
        h.clear();

        i.setId("I1");
        i.setHFilterId("H1");
        i.insert();
        i.clear();

        h.setIn(i).add("id", "hFilterId");
        assertEquals(1, h.count());

    }

    @TestTemplate
    void testExceptionWhileAddingNotExistedFieldsToLookupForTable(CallContext context) {
        AFilterCursor a = new AFilterCursor(context);
        BFilterCursor b = new BFilterCursor(context);
        _testExceptionWhileAddingNotExistedFieldsToLookup(a, b);

    }

    @TestTemplate
    void testExceptionWhileAddingNotExistedFieldsToLookupForView(CallContext context) {
        AFilterViewCursor a = new AFilterViewCursor(context);
        BFilterViewCursor b = new BFilterViewCursor(context);
        _testExceptionWhileAddingNotExistedFieldsToLookup(a, b);


    }

    @TestTemplate
    void testExceptionWhileAddingFieldsWithNotMatchesTypesToLookupForTable(CallContext context) {
        AFilterCursor a = new AFilterCursor(context);
        BFilterCursor b = new BFilterCursor(context);
        _testExceptionWhileAddingFieldsWithNotMatchesTypesToLookup(a, b);

    }

    @TestTemplate
    void testExceptionWhileAddingFieldsWithNotMatchesTypesToLookupForView(CallContext context) {
        AFilterViewCursor a = new AFilterViewCursor(context);
        BFilterViewCursor b = new BFilterViewCursor(context);
        _testExceptionWhileAddingFieldsWithNotMatchesTypesToLookup(a, b);

    }

    @TestTemplate
    void testExceptionWhileAddingFieldsWithoutIndexToLookup(CallContext context) {
        AFilterCursor a = new AFilterCursor(context);
        BFilterCursor b = new BFilterCursor(context);

        FieldsLookup lookup = a.setIn(b);

        assertThrows(CelestaException.class,
                () -> lookup.add("noIndexA", "numb1"));

        assertThrows(CelestaException.class,
                () -> lookup.add("number1", "noIndexB"));
        assertThrows(CelestaException.class,
                () -> lookup.add("noIndexA", "noIndexB"));
    }

    @TestTemplate
    void testExceptionWhenPairsFromLookupDoNotMatchToIndices(CallContext context) {
        AFilterCursor a = new AFilterCursor(context);
        BFilterCursor b = new BFilterCursor(context);

        FieldsLookup lookup = a.setIn(b);

        assertThrows(CelestaException.class, () -> lookup.add("number1", "numb2"));
        assertThrows(CelestaException.class, () -> lookup.add("number2", "numb1"));

        assertThrows(CelestaException.class,
                () -> lookup.add("date", "created").add("number2", "numb2"));


    }

    void _fillTablesForTestInFilterWithRangeOnOtherCursor(AFilterCursor a, BFilterCursor b, Timestamp timestamp) {
        a.setDate(timestamp);
        a.setNumber1(5);
        a.setNumber2(-10);
        a.insert();
        a.clear();

        a.setDate(timestamp);
        a.setNumber1(6);
        a.setNumber2(-20);
        a.insert();
        a.clear();

        a.setDate(timestamp);
        a.setNumber1(1);
        a.setNumber2(-20);
        a.insert();
        a.clear();

        a.setDate(Timestamp.valueOf(LocalDateTime.now().plusDays(1)));
        a.setNumber2(-30);
        a.insert();
        a.clear();

        b.setCreated(timestamp);
        b.setNumb1(6);
        b.setNumb2(-40);
        b.insert();
        b.clear();

        b.setCreated(timestamp);
        b.setNumb1(5);
        b.setNumb2(-40);
        b.insert();
        b.clear();

        b.setCreated(timestamp);
        b.setNumb1(1);
        b.setNumb2(-41);
        b.insert();
        b.clear();

    }

    void _testInFilterForIndices(CallContext context, BasicCursor a, BasicCursor b) throws ParseException {
        AFilterCursor aTableCursor = new AFilterCursor(context);
        BFilterCursor bTableCursor = new BFilterCursor(context);

        aTableCursor.deleteAll();
        bTableCursor.deleteAll();

        Timestamp timestamp = Timestamp.valueOf(LocalDateTime.now());

        aTableCursor.setDate(timestamp);
        aTableCursor.setNumber1(5);
        aTableCursor.setNumber2(-10);
        aTableCursor.insert();
        aTableCursor.clear();

        aTableCursor.setDate(timestamp);
        aTableCursor.setNumber1(1);
        aTableCursor.setNumber2(-20);
        aTableCursor.insert();
        aTableCursor.clear();

        aTableCursor.setDate(Timestamp.valueOf(LocalDateTime.now().plusDays(1)));
        aTableCursor.setNumber2(-30);
        aTableCursor.insert();
        aTableCursor.clear();

        bTableCursor.setCreated(timestamp);
        bTableCursor.setNumb1(2);
        bTableCursor.setNumb2(-40);
        bTableCursor.insert();
        bTableCursor.clear();

        bTableCursor.setCreated(timestamp);
        bTableCursor.setNumb1(5);
        bTableCursor.setNumb2(-50);
        bTableCursor.insert();
        bTableCursor.clear();

        ((InFilterSupport) a).setIn(b).add("date", "created");
        assertEquals(2, a.count());

        ((InFilterSupport) a).setIn(b).add("date", "created").add("number1", "numb1");
        assertEquals(1, a.count());

        ((InFilterSupport) a).setIn(b).add("date", "created").add("number1", "numb1").add("number2", "numb2");
        assertEquals(0, a.count());

    }

    void _testInFilterWithRangeInMainCursor(CallContext context, BasicCursor a, BasicCursor b) throws ParseException {
        AFilterCursor aTableCursor = new AFilterCursor(context);
        BFilterCursor bTableCursor = new BFilterCursor(context);

        aTableCursor.deleteAll();
        bTableCursor.deleteAll();

        Timestamp timestamp = Timestamp.valueOf(LocalDateTime.now());

        aTableCursor.setDate(timestamp);
        aTableCursor.setNumber1(5);
        aTableCursor.setNumber2(-10);
        aTableCursor.insert();
        aTableCursor.clear();

        aTableCursor.setDate(timestamp);
        aTableCursor.setNumber1(1);
        aTableCursor.setNumber2(-20);
        aTableCursor.insert();
        aTableCursor.clear();

        aTableCursor.setDate(Timestamp.valueOf(LocalDateTime.now().plusDays(1)));
        aTableCursor.setNumber2(-30);
        aTableCursor.insert();
        aTableCursor.clear();

        bTableCursor.setCreated(timestamp);
        bTableCursor.setNumb1(2);
        bTableCursor.setNumb2(-40);
        bTableCursor.insert();
        bTableCursor.clear();

        bTableCursor.setCreated(timestamp);
        bTableCursor.setNumb1(5);
        bTableCursor.setNumb2(-50);
        bTableCursor.insert();
        bTableCursor.clear();

        a.setRange("number1", 5);
        ((InFilterSupport) a).setIn(b).add("date", "created");
        assertEquals(1, a.count());
        a.first();


    }

    void checkFirstRec(BasicCursor a) {
        if (a instanceof AFilterCursor) {
            assertEquals(5, ((AFilterCursor) a).getNumber1().intValue());
            assertEquals(-10, ((AFilterCursor) a).getNumber2().intValue());
        } else {
            assertEquals(5, ((AFilterViewCursor) a).getNumber1().intValue());
            assertEquals(-10, ((AFilterViewCursor) a).getNumber2().intValue());
        }
    }

    void checkSecondRec(BasicCursor a) {
        if (a instanceof AFilterCursor) {
            assertEquals(6, ((AFilterCursor) a).getNumber1().intValue());
            assertEquals(-20, ((AFilterCursor) a).getNumber2().intValue());
        } else {
            assertEquals(6, ((AFilterViewCursor) a).getNumber1().intValue());
            assertEquals(-20, ((AFilterViewCursor) a).getNumber2().intValue());
        }
    }

    void _testInFilterWithRangeInOtherCursorBeforeSetIn(CallContext context, BasicCursor a, BasicCursor b) throws ParseException {
        AFilterCursor aTableCursor = new AFilterCursor(context);
        BFilterCursor bTableCursor = new BFilterCursor(context);

        aTableCursor.deleteAll();
        bTableCursor.deleteAll();

        Timestamp timestamp = Timestamp.valueOf(LocalDateTime.now());

        _fillTablesForTestInFilterWithRangeOnOtherCursor(aTableCursor, bTableCursor, timestamp);

        b.setRange("numb2", -40);
        ((InFilterSupport) a).setIn(b).add("date", "created").add("number1", "numb1");

        assertEquals(2, a.count());

        a.first();
        checkFirstRec(a);

        a.navigate(">");
        checkSecondRec(a);

    }

    void _testInFilterWithRangeInOtherCursorAfterSetIn(CallContext context, BasicCursor a, BasicCursor b) throws ParseException {
        AFilterCursor aTableCursor = new AFilterCursor(context);
        BFilterCursor bTableCursor = new BFilterCursor(context);

        aTableCursor.deleteAll();
        bTableCursor.deleteAll();

        Timestamp timestamp = Timestamp.valueOf(LocalDateTime.now());

        _fillTablesForTestInFilterWithRangeOnOtherCursor(aTableCursor, bTableCursor, timestamp);


        ((InFilterSupport) a).setIn(b).add("date", "created").add("number1", "numb1");

        assertEquals(3, a.count());

        b.setRange("numb2", -40);
        assertEquals(2, a.count());

        a.first();
        checkFirstRec(a);


        a.navigate(">");
        checkSecondRec(a);

    }

    void _testInFilterWithAdditionalLookup(CallContext context, BasicCursor a, BasicCursor b, BasicCursor g) throws ParseException {
        AFilterCursor aTableCursor = new AFilterCursor(context);
        BFilterCursor bTableCursor = new BFilterCursor(context);
        GFilterCursor gTableCursor = new GFilterCursor(context);

        aTableCursor.deleteAll();
        bTableCursor.deleteAll();
        gTableCursor.deleteAll();

        Timestamp timestamp = Timestamp.valueOf(LocalDateTime.now());

        _fillTablesForTestInFilterWithRangeOnOtherCursor(aTableCursor, bTableCursor, timestamp);

        gTableCursor.setCreateDate(timestamp);
        gTableCursor.setNum1(5);
        gTableCursor.setNum2(-30);
        gTableCursor.insert();
        gTableCursor.clear();

        gTableCursor.setCreateDate(timestamp);
        gTableCursor.setNum1(6);
        gTableCursor.setNum2(-40);
        gTableCursor.insert();
        gTableCursor.clear();

        gTableCursor.setCreateDate(timestamp);
        gTableCursor.setNum1(1);
        gTableCursor.setNum2(-41);
        gTableCursor.insert();
        gTableCursor.clear();

        gTableCursor.setCreateDate(timestamp);
        gTableCursor.setNum1(1);
        gTableCursor.setNum2(-42);
        gTableCursor.insert();
        gTableCursor.clear();


        FieldsLookup lookup = ((InFilterSupport) a).setIn(b).add("date", "created").add("number1", "numb1");
        lookup.and(g).add("date", "createDate").add("number1", "num1");

        assertEquals(3, a.count());

        b.setRange("numb2", -40);
        assertEquals(2, a.count());

        a.first();
        checkFirstRec(a);

        a.navigate(">");
        checkSecondRec(a);

        g.setRange("num2", -30);
        assertEquals(1, a.count());

        a.first();
        checkFirstRec(a);

    }

    void _testExceptionWhileAddingNotExistedFieldsToLookup(InFilterSupport a, BasicCursor b) {
        FieldsLookup lookup = a.setIn(b);
        assertThrows(ParseException.class,
                () -> lookup.add("notExistingField", "created"));
        assertThrows(ParseException.class,
                () -> lookup.add("date", "notExistingField"));
        assertThrows(ParseException.class,
                () -> lookup.add("notExistingField", "notExistingField"));


    }

    void _testExceptionWhileAddingFieldsWithNotMatchesTypesToLookup(InFilterSupport a, BasicCursor b) {
        FieldsLookup lookup = a.setIn(b);
        assertThrows(CelestaException.class,
                () -> lookup.add("date", "numb1"));
    }

    @TestTemplate
    void complexFilterWorksForTableCursor(CallContext context) {
        AFilterCursor a = new AFilterCursor(context);
        a.deleteAll();
        a.clear();
        a.setNumber1(1);
        a.setNumber1(2);
        a.insert();

        a.clear();
        a.setNumber1(2);
        a.setNumber2(1);
        a.insert();

        assertEquals(2, a.count());
        a.setComplexFilter("number1 > number2");
        assertEquals(1, a.count());
        a.first();
        assertEquals(2, a.getNumber1().intValue());
    }
}

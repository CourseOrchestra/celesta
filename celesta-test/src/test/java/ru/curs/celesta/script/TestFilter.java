package ru.curs.celesta.script;

import filters.AFilterCursor;
import filters.AFilterViewCursor;
import filters.BFilterCursor;
import filters.BFilterViewCursor;
import filters.CFilterCursor;
import filters.DFilterCursor;
import filters.EFilterCursor;
import filters.FFilterCursor;
import filters.GFilterCursor;
import filters.GFilterViewCursor;
import filters.HFilterCursor;
import filters.IFilterCursor;
import org.junit.jupiter.api.TestTemplate;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.InFilterSupport;
import ru.curs.celesta.dbutils.filter.value.FieldsLookup;

import java.sql.Timestamp;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class TestFilter implements ScriptTest {

    @TestTemplate
    public void testInFilterForTable(CallContext context) {
        AFilterCursor a = new AFilterCursor(context);
        BFilterCursor b = new BFilterCursor(context);
        _testInFilterForIndices(context, a, b);
    }

    @TestTemplate
    public void testInFilterForView(CallContext context) {
        AFilterViewCursor a = new AFilterViewCursor(context);
        BFilterViewCursor b = new BFilterViewCursor(context);
        _testInFilterForIndices(context, a, b);
    }

    @TestTemplate
    public void testInFilterForTableToView(CallContext context) {
        AFilterCursor a = new AFilterCursor(context);
        BFilterViewCursor b = new BFilterViewCursor(context);
        _testInFilterForIndices(context, a, b);
    }

    @TestTemplate
    public void testInFilterForViewToTable(CallContext context) {
        AFilterCursor a = new AFilterCursor(context);
        BFilterViewCursor b = new BFilterViewCursor(context);
        _testInFilterForIndices(context, a, b);
    }

    @TestTemplate
    public void testInFilterForSimplePks(CallContext context) {
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

        c.setIn(d).add(c.COLUMNS.id(), d.COLUMNS.id());
        assertEquals(2, c.count());
    }

    @TestTemplate
    public void testInFilterForComplexPks(CallContext context) {
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

        e.setIn(f).add(e.COLUMNS.id(), f.COLUMNS.id()).add(e.COLUMNS.number(), f.COLUMNS.numb());
        assertEquals(2, e.count());
    }

    @TestTemplate
    public void testInFilterWithRangeInMainCursorForTable(CallContext context) {
        AFilterCursor a = new AFilterCursor(context);
        BFilterCursor b = new BFilterCursor(context);
        _testInFilterWithRangeInMainCursor(context, a, b);
    }

    @TestTemplate
    public void testInFilterWithRangeInMainCursorForView(CallContext context) {
        AFilterViewCursor a = new AFilterViewCursor(context);
        BFilterViewCursor b = new BFilterViewCursor(context);
        _testInFilterWithRangeInMainCursor(context, a, b);
    }

    @TestTemplate
    public void testInFilterWithRangeInMainCursorForTableToView(CallContext context) {
        AFilterCursor a = new AFilterCursor(context);
        BFilterViewCursor b = new BFilterViewCursor(context);
        _testInFilterWithRangeInMainCursor(context, a, b);
    }


    @TestTemplate
    public void testInFilterWithRangeInOtherCursorBeforeSetInForTable(CallContext context) {
        AFilterCursor a = new AFilterCursor(context);
        BFilterCursor b = new BFilterCursor(context);
        _testInFilterWithRangeInOtherCursorBeforeSetIn(context, a, b);
    }

    @TestTemplate
    public void testInFilterWithRangeInOtherCursorBeforeSetInForView(CallContext context) {
        AFilterViewCursor a = new AFilterViewCursor(context);
        BFilterViewCursor b = new BFilterViewCursor(context);
        _testInFilterWithRangeInOtherCursorBeforeSetIn(context, a, b);
    }

    @TestTemplate
    public void testInFilterWithRangeInOtherCursorBeforeSetInForTableToView(CallContext context) {
        AFilterCursor a = new AFilterCursor(context);
        BFilterViewCursor b = new BFilterViewCursor(context);
        _testInFilterWithRangeInOtherCursorBeforeSetIn(context, a, b);
    }

    @TestTemplate
    public void testInFilterWithRangeInOtherCursorAfterSetInForTable(CallContext context) {
        AFilterCursor a = new AFilterCursor(context);
        BFilterCursor b = new BFilterCursor(context);
        _testInFilterWithRangeInOtherCursorAfterSetIn(context, a, b);
    }

    @TestTemplate
    public void testInFilterWithRangeInOtherCursorAfterSetInForView(CallContext context) {
        AFilterViewCursor a = new AFilterViewCursor(context);
        BFilterViewCursor b = new BFilterViewCursor(context);
        _testInFilterWithRangeInOtherCursorAfterSetIn(context, a, b);
    }

    @TestTemplate
    public void testInFilterWithAdditionalLookupForTable(CallContext context) {
        AFilterCursor a = new AFilterCursor(context);
        BFilterCursor b = new BFilterCursor(context);
        GFilterCursor g = new GFilterCursor(context);

        _testInFilterWithAdditionalLookup(context, a, b, g);
    }

    @TestTemplate
    public void testInFilterWithAdditionalLookupForView(CallContext context) {
        AFilterViewCursor a = new AFilterViewCursor(context);
        BFilterViewCursor b = new BFilterViewCursor(context);
        GFilterViewCursor g = new GFilterViewCursor(context);
        _testInFilterWithAdditionalLookup(context, a, b, g);
    }

    @TestTemplate
    public void testInFilterWithAdditionalLookupForTableToView(CallContext context) {
        AFilterCursor a = new AFilterCursor(context);
        BFilterViewCursor b = new BFilterViewCursor(context);
        GFilterViewCursor g = new GFilterViewCursor(context);
        _testInFilterWithAdditionalLookup(context, a, b, g);
    }

    @TestTemplate
    public void testInFilterWhenTargetHasPkAndOtherHasPkWithNotSameOrderAndIndexWithSameOrder(CallContext context) {
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

        h.setIn(i).add(h.COLUMNS.id(), i.COLUMNS.hFilterId());
        assertEquals(1, h.count());
    }

    @TestTemplate
    public void testExceptionWhileAddingNotExistedFieldsToLookupForTable(CallContext context) {
        AFilterCursor a = new AFilterCursor(context);
        BFilterCursor b = new BFilterCursor(context);
        _testExceptionWhileAddingNotExistedFieldsToLookup(a, b);
    }

    @TestTemplate
    public void testExceptionWhileAddingNotExistedFieldsToLookupForView(CallContext context) {
        AFilterViewCursor a = new AFilterViewCursor(context);
        BFilterViewCursor b = new BFilterViewCursor(context);
        _testExceptionWhileAddingNotExistedFieldsToLookup(a, b);
    }

    @TestTemplate
    public void testExceptionWhileAddingFieldsWithNotMatchesTypesToLookupForTable(CallContext context) {
        AFilterCursor a = new AFilterCursor(context);
        BFilterCursor b = new BFilterCursor(context);
        _testExceptionWhileAddingFieldsWithNotMatchesTypesToLookup(a, b);
    }

    @TestTemplate
    public void testExceptionWhileAddingFieldsWithNotMatchesTypesToLookupForView(CallContext context) {
        AFilterViewCursor a = new AFilterViewCursor(context);
        BFilterViewCursor b = new BFilterViewCursor(context);
        _testExceptionWhileAddingFieldsWithNotMatchesTypesToLookup(a, b);
    }

    @TestTemplate
    public void testExceptionWhileAddingFieldsWithoutIndexToLookup(CallContext context) {
        AFilterCursor a = new AFilterCursor(context);
        BFilterCursor b = new BFilterCursor(context);

        FieldsLookup lookup = a.setIn(b);

        assertThrows(CelestaException.class,
                () -> lookup.add("noIndexA", b.COLUMNS.numb1().getName()));

        assertThrows(CelestaException.class,
                () -> lookup.add(a.COLUMNS.number1().getName(), "noIndexB"));

        assertThrows(CelestaException.class,
                () -> lookup.add("noIndexA", "noIndexB"));
    }

    @TestTemplate
    public void testExceptionWhenPairsFromLookupDoNotMatchToIndices(CallContext context) {
        AFilterCursor a = new AFilterCursor(context);
        BFilterCursor b = new BFilterCursor(context);

        FieldsLookup lookup = a.setIn(b);

        assertThrows(CelestaException.class, () -> lookup.add(a.COLUMNS.number1(), b.COLUMNS.numb2()));
        assertThrows(CelestaException.class, () -> lookup.add(a.COLUMNS.number2(), b.COLUMNS.numb1()));

        assertThrows(CelestaException.class,
                () -> lookup.add(a.COLUMNS.date(), b.COLUMNS.created())
                            .add(a.COLUMNS.number2(), b.COLUMNS.numb2()));
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

    void _testInFilterForIndices(CallContext context, BasicCursor a, BasicCursor b) {
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

    void _testInFilterWithRangeInMainCursor(CallContext context, BasicCursor a, BasicCursor b) {
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

    private void checkFirstRec(BasicCursor a) {
        if (a instanceof AFilterCursor) {
            assertEquals(5, ((AFilterCursor) a).getNumber1().intValue());
            assertEquals(-10, ((AFilterCursor) a).getNumber2().intValue());
        } else {
            assertEquals(5, ((AFilterViewCursor) a).getNumber1().intValue());
            assertEquals(-10, ((AFilterViewCursor) a).getNumber2().intValue());
        }
    }

    private void checkSecondRec(BasicCursor a) {
        if (a instanceof AFilterCursor) {
            assertEquals(6, ((AFilterCursor) a).getNumber1().intValue());
            assertEquals(-20, ((AFilterCursor) a).getNumber2().intValue());
        } else {
            assertEquals(6, ((AFilterViewCursor) a).getNumber1().intValue());
            assertEquals(-20, ((AFilterViewCursor) a).getNumber2().intValue());
        }
    }

    void _testInFilterWithRangeInOtherCursorBeforeSetIn(CallContext context, BasicCursor a, BasicCursor b) {
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

    void _testInFilterWithRangeInOtherCursorAfterSetIn(CallContext context, BasicCursor a, BasicCursor b) {
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

    void _testInFilterWithAdditionalLookup(CallContext context, BasicCursor a, BasicCursor b, BasicCursor g) {
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
        assertThrows(CelestaException.class,
                () -> lookup.add("notExistingField", "created"));
        assertThrows(CelestaException.class,
                () -> lookup.add("date", "notExistingField"));
        assertThrows(CelestaException.class,
                () -> lookup.add("notExistingField", "notExistingField"));
    }

    void _testExceptionWhileAddingFieldsWithNotMatchesTypesToLookup(InFilterSupport a, BasicCursor b) {
        FieldsLookup lookup = a.setIn(b);
        assertThrows(CelestaException.class,
                () -> lookup.add("date", "numb1"));
    }

    @TestTemplate
    public void complexFilterWorksForTableCursor(CallContext context) {
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

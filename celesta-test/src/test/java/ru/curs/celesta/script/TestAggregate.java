package ru.curs.celesta.script;

import aggregate.*;
import org.junit.jupiter.api.TestTemplate;
import ru.curs.celesta.CallContext;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;


class TestAggregate implements ScriptTest {

    @TestTemplate
    void test_count_without_condition(CallContext context) {
        CountConditionLessCursor tableCursor = new CountConditionLessCursor(context);
        tableCursor.deleteAll();

        ViewCountCondLessCursor viewCursor = new ViewCountCondLessCursor(context);
        assertEquals(1, viewCursor.count());
        viewCursor.first();
        assertEquals(0, viewCursor.getC().intValue());

        tableCursor.insert();
        tableCursor.clear();
        tableCursor.insert();

        assertEquals(1, viewCursor.count());
        viewCursor.first();
        assertEquals(2, viewCursor.getC().intValue());


    }

    @TestTemplate
    void test_count_with_getdate_condition(CallContext context) {
        CountGetDateCondCursor tableCursor = new CountGetDateCondCursor(context);
        tableCursor.deleteAll();

        ViewCountGetDateCondCursor viewCursor = new ViewCountGetDateCondCursor(context);
        viewCursor.first();
        assertEquals(0, viewCursor.getC().intValue());

        tableCursor.insert();
        tableCursor.clear();
        tableCursor.setDate(Timestamp.valueOf(LocalDateTime.now().minusSeconds(2)));
        tableCursor.insert();

        viewCursor.first();
        assertEquals(0, viewCursor.getC().intValue());

        tableCursor.clear();
        tableCursor.setDate(Timestamp.valueOf(LocalDateTime.now().plusDays(1)));
        tableCursor.insert();

        viewCursor.first();
        assertEquals(1, viewCursor.getC().intValue());
    }

    @TestTemplate
    void test_sum_one_field(CallContext context) {
        TableSumOneFieldCursor tableOneFieldCursor = new TableSumOneFieldCursor(context);
        tableOneFieldCursor.deleteAll();

        ViewSumOneFieldCursor viewOneFieldCursor = new ViewSumOneFieldCursor(context);
        SumFieldAndNumberCursor viewOneFieldAndNumberCursor = new SumFieldAndNumberCursor(context);
        ViewSumTwoNumbersCursor viewTwoNumbersCursor = new ViewSumTwoNumbersCursor(context);

        assertEquals(1, viewOneFieldCursor.count());
        assertEquals(1, viewOneFieldAndNumberCursor.count());
        assertEquals(1, viewTwoNumbersCursor.count());

        viewOneFieldCursor.first();
        viewOneFieldAndNumberCursor.first();
        viewTwoNumbersCursor.first();
        assertEquals(null, viewOneFieldCursor.getS());
        assertEquals(null, viewOneFieldAndNumberCursor.getS());
        assertEquals(null, viewTwoNumbersCursor.getS());

        tableOneFieldCursor.setF(4);
        tableOneFieldCursor.insert();

        viewOneFieldCursor.first();
        viewOneFieldAndNumberCursor.first();
        viewTwoNumbersCursor.first();
        assertEquals(4, viewOneFieldCursor.getS().intValue());
        assertEquals(5, viewOneFieldAndNumberCursor.getS().intValue());
        assertEquals(3, viewTwoNumbersCursor.getS().intValue());


    }

    @TestTemplate
    void test_sum_two_fields(CallContext context) {
        TableSumTwoFieldsCursor tableTwoFieldsCursor = new TableSumTwoFieldsCursor(context);
        tableTwoFieldsCursor.deleteAll();

        ViewSumTwoFieldsCursor viewTwoFieldsCursor = new ViewSumTwoFieldsCursor(context);

        assertEquals(1, viewTwoFieldsCursor.count());

        viewTwoFieldsCursor.first();
        assertEquals(null, viewTwoFieldsCursor.getS());

        tableTwoFieldsCursor.setF1(2);
        tableTwoFieldsCursor.insert();
        tableTwoFieldsCursor.clear();

        viewTwoFieldsCursor.first();
        assertEquals(null, viewTwoFieldsCursor.getS());

        tableTwoFieldsCursor.setF2(2);
        tableTwoFieldsCursor.insert();
        tableTwoFieldsCursor.clear();

        viewTwoFieldsCursor.first();
        assertEquals(null, viewTwoFieldsCursor.getS());

        tableTwoFieldsCursor.setF1(2);
        tableTwoFieldsCursor.setF2(3);
        tableTwoFieldsCursor.insert();
        tableTwoFieldsCursor.clear();

        viewTwoFieldsCursor.first();
        assertEquals(5, viewTwoFieldsCursor.getS().intValue());


    }

    @TestTemplate
    void test_min_and_max_one_field(CallContext context) {
        TableMinMaxCursor tableCur = new TableMinMaxCursor(context);
        tableCur.deleteAll();

        ViewMinOneFieldCursor viewMinOneFieldCur = new ViewMinOneFieldCursor(context);
        ViewMaxOneFieldCursor viewMaxOneFieldCur = new ViewMaxOneFieldCursor(context);
        ViewMinTwoFieldsCursor viewMinTwoFieldsCur = new ViewMinTwoFieldsCursor(context);
        ViewMaxTwoFieldsCursor viewMaxTwoFieldsCur = new ViewMaxTwoFieldsCursor(context);
        ViewCountMinMaxCursor viewCountMinMaxCur = new ViewCountMinMaxCursor(context);

        viewMinOneFieldCur.first();
        viewMaxOneFieldCur.first();
        viewMinTwoFieldsCur.first();
        viewMaxTwoFieldsCur.first();
        viewCountMinMaxCur.first();
        assertEquals(1, viewMinOneFieldCur.count());
        assertEquals(1, viewMaxOneFieldCur.count());
        assertEquals(1, viewMinTwoFieldsCur.count());
        assertEquals(1, viewMaxTwoFieldsCur.count());
        assertEquals(1, viewCountMinMaxCur.count());
        assertEquals(null, viewMinOneFieldCur.getM());
        assertEquals(null, viewMaxOneFieldCur.getM());
        assertEquals(null, viewMinTwoFieldsCur.getM());
        assertEquals(null, viewMaxTwoFieldsCur.getM());
        assertEquals(0, viewCountMinMaxCur.getCountv().intValue());
        assertEquals(null, viewCountMinMaxCur.getMaxv());
        assertEquals(null, viewCountMinMaxCur.getMinv());

        tableCur.setF1(1);
        tableCur.setF2(5);
        tableCur.insert();
        tableCur.clear();

        tableCur.setF1(5);
        tableCur.setF2(2);
        tableCur.insert();
        tableCur.clear();

        viewMinOneFieldCur.first();
        viewMaxOneFieldCur.first();
        viewMinTwoFieldsCur.first();
        viewMaxTwoFieldsCur.first();
        viewCountMinMaxCur.first();

        assertEquals(1, viewMinOneFieldCur.getM().intValue());
        assertEquals(5, viewMaxOneFieldCur.getM().intValue());
        assertEquals(6, viewMinTwoFieldsCur.getM().intValue());
        assertEquals(7, viewMaxTwoFieldsCur.getM().intValue());
        assertEquals(2, viewCountMinMaxCur.getCountv().intValue());
        assertEquals(5, viewCountMinMaxCur.getMaxv().intValue());
        assertEquals(2, viewCountMinMaxCur.getMinv().intValue());


    }

    @TestTemplate
    void testGroupBy(CallContext context) {
        TableGroupByCursor tableCursor = new TableGroupByCursor(context);
        tableCursor.deleteAll();

        ViewGroupByCursor viewGroupByCur = new ViewGroupByCursor(context);
        ViewGroupByAggregateCursor viewAggregateCursor = new ViewGroupByAggregateCursor(context);

        assertEquals(0, viewAggregateCursor.count());

        String name1 = "A";
        String name2 = "B";

        tableCursor.setName(name1);
        tableCursor.setCost(100);
        tableCursor.insert();
        tableCursor.clear();

        viewGroupByCur.first();
        assertEquals(name1, viewGroupByCur.getName());
        assertEquals(100, viewGroupByCur.getCost().intValue());

        tableCursor.setName(name1);
        tableCursor.setCost(150);
        tableCursor.insert();
        tableCursor.clear();
        tableCursor.setName(name2);
        tableCursor.setCost(50);
        tableCursor.insert();
        tableCursor.clear();

        assertEquals(2, viewAggregateCursor.count());
        viewAggregateCursor.first();
        assertEquals(name1, viewAggregateCursor.getName());
        assertEquals(250, viewAggregateCursor.getS().intValue());

        viewAggregateCursor.next();
        assertEquals(name2, viewAggregateCursor.getName());
        assertEquals(50, viewAggregateCursor.getS().intValue());

    }

    @TestTemplate
    void testSumOfDecimal(CallContext context) {
        TWithDecimalCursor t = new TWithDecimalCursor(context);
        ViewWithDecimalCursor v = new ViewWithDecimalCursor(context);

        t.insert();
        t.clear();
        t.insert();

        v.first();

        assertEquals(new BigDecimal("48.02"), v.getF1().stripTrailingZeros());
        assertEquals(new BigDecimal("2.0002"), v.getF2().stripTrailingZeros());
        assertEquals(new BigDecimal("50.0202"), v.getF12().stripTrailingZeros());
    }
}

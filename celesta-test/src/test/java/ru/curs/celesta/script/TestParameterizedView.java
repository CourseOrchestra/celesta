package ru.curs.celesta.script;

import org.junit.jupiter.api.TestTemplate;
import pView.PView1Cursor;
import pView.PView2Cursor;
import pView.PView3Cursor;
import pView.PView4Cursor;
import pView.PView5Cursor;
import pView.T1Cursor;
import pView.T2Cursor;
import pView.T3Cursor;
import ru.curs.celesta.CallContext;

import java.math.BigDecimal;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestParameterizedView implements ScriptTest {

    @TestTemplate
    void test_p_view_with_aggregate(CallContext context) {
        T1Cursor tCursor = new T1Cursor(context);
        tCursor.deleteAll();

        tCursor.setF1(1);
        tCursor.setF2(2);
        tCursor.setF3("A");
        tCursor.setF4(0.5);
        tCursor.insert();
        tCursor.clear();

        tCursor.setF1(9);
        tCursor.setF2(2);
        tCursor.setF3("A");
        tCursor.setF4(0.2);
        tCursor.insert();
        tCursor.clear();

        tCursor.setF1(4);
        tCursor.setF2(3);
        tCursor.setF3("A");
        tCursor.setF4(0.1);
        tCursor.insert();
        tCursor.clear();

        tCursor.setF1(7);
        tCursor.setF2(2);
        tCursor.setF3("B");
        tCursor.insert();
        tCursor.clear();


        PView1Cursor pvCursor = new PView1Cursor(context, 1);
        assertEquals(0, pvCursor.count());

        pvCursor = new PView1Cursor(context, 2);
        pvCursor.orderBy(pvCursor.COLUMNS.f3());
        assertEquals(2, pvCursor.count());

        pvCursor.first();
        assertEquals(10, pvCursor.getSumv().intValue());
        assertEquals("A", pvCursor.getF3());
        pvCursor.next();
        assertEquals(7, pvCursor.getSumv().intValue());
        assertEquals("B", pvCursor.getF3());

        pvCursor = new PView1Cursor(context, 3);
        assertEquals(1, pvCursor.count());

        pvCursor.first();
        assertEquals(4, pvCursor.getSumv().intValue());
        assertEquals("A", pvCursor.getF3());

        PView5Cursor pv5 = new PView5Cursor(context, 9);
        pv5.first();
        assertEquals(0.4, pv5.getC(), 0.00001);
    }

    @TestTemplate
    void test_p_view_with_two_parameters(CallContext context) {
        T1Cursor tCursor = new T1Cursor(context);
        tCursor.deleteAll();

        tCursor.setF1(1);
        tCursor.setF2(2);
        tCursor.setF3("A");
        tCursor.insert();
        tCursor.clear();

        tCursor.setF1(9);
        tCursor.setF2(2);
        tCursor.setF3("A");
        tCursor.insert();
        tCursor.clear();

        tCursor.setF1(4);
        tCursor.setF2(3);
        tCursor.setF3("A");
        tCursor.insert();
        tCursor.clear();

        tCursor.setF1(7);
        tCursor.setF2(2);
        tCursor.setF3("B");
        tCursor.insert();
        tCursor.clear();

        HashMap<String, Object> param = new HashMap<>();
        param.put("param", 2);
        param.put("param2", "C");
        PView2Cursor pvCursor = new PView2Cursor(context, param);
        assertEquals(0, pvCursor.count());

        param = new HashMap<>();
        param.put("param", 2);
        param.put("param2", "A");
        pvCursor = new PView2Cursor(context, param);
        pvCursor.orderBy(pvCursor.COLUMNS.f1());
        assertEquals(2, pvCursor.count());

        pvCursor.first();
        assertEquals(1, pvCursor.getF1().intValue());
        assertEquals(2, pvCursor.getF2().intValue());
        assertEquals("A", pvCursor.getF3());
        pvCursor.next();
        assertEquals(9, pvCursor.getF1().intValue());
        assertEquals(2, pvCursor.getF2().intValue());
        assertEquals("A", pvCursor.getF3());

        param = new HashMap<>();
        param.put("param", 3);
        param.put("param2", "A");
        pvCursor = new PView2Cursor(context, param);
        assertEquals(1, pvCursor.count());

        pvCursor.first();
        assertEquals(4, pvCursor.getF1().intValue());
        assertEquals(3, pvCursor.getF2().intValue());
        assertEquals("A", pvCursor.getF3());


        param = new HashMap<>();
        param.put("param", 2);
        param.put("param2", "B");
        pvCursor = new PView2Cursor(context, param);
        assertEquals(1, pvCursor.count());

        pvCursor.first();
        assertEquals(7, pvCursor.getF1().intValue());
        assertEquals(2, pvCursor.getF2().intValue());
        assertEquals("B", pvCursor.getF3());

    }

    @TestTemplate
    void test_p_view_with_join(CallContext context) {
        T1Cursor tCursor1 = new T1Cursor(context);
        tCursor1.deleteAll();

        T2Cursor tCursor2 = new T2Cursor(context);
        tCursor2.deleteAll();

        tCursor1.setF1(1);
        tCursor1.setF2(2);
        tCursor1.setF3("A");
        tCursor1.insert();
        tCursor1.clear();

        tCursor1.setF1(9);
        tCursor1.setF2(2);
        tCursor1.setF3("A");
        tCursor1.insert();
        tCursor1.clear();


        HashMap<String, Object> param = new HashMap<>();
        param.put("p", 2);
        PView3Cursor pvCursor = new PView3Cursor(context, param);
        assertEquals(1, pvCursor.count());
        pvCursor.first();
        assertEquals(2, pvCursor.getC().intValue());

        tCursor2.setFf1(9);
        tCursor2.setFf2(3);
        tCursor2.setFf3("B");
        tCursor2.insert();
        tCursor2.clear();

        assertEquals(1, pvCursor.count());
        pvCursor.first();
        assertEquals(2, pvCursor.getC().intValue());

        tCursor2.setFf1(9);
        tCursor2.setFf2(2);
        tCursor2.setFf3("B");
        tCursor2.insert();
        tCursor2.clear();

        assertEquals(1, pvCursor.count());
        pvCursor.first();
        assertEquals(2, pvCursor.getC().intValue());

        tCursor2.setFf1(9);
        tCursor2.setFf2(2);
        tCursor2.setFf3("B");
        tCursor2.insert();
        tCursor2.clear();

        // Проверка на декартово произведение;
        assertEquals(1, pvCursor.count());
        pvCursor.first();
        assertEquals(4, pvCursor.getC().intValue());

    }

    @TestTemplate
    void testSumOfDecimal(CallContext context) {
        T3Cursor t = new T3Cursor(context);

        t.insert();
        t.clear();

        t.setF2(new BigDecimal("0.0001"));
        t.insert();

        HashMap<String, Object> param = new HashMap<>();
        param.put("p", new BigDecimal("1.00001"));
        PView4Cursor pv = new PView4Cursor(context, param);
        pv.first();

        assertEquals(new BigDecimal("24.01"), pv.getF1().stripTrailingZeros());
        assertEquals(new BigDecimal("1.0001"), pv.getF2().stripTrailingZeros());
        assertEquals(new BigDecimal("25.0101"), pv.getF12().stripTrailingZeros());

        param = new HashMap<>();
        param.put("p", new BigDecimal("0.00001"));
        pv = new PView4Cursor(context, param);
        pv.first();

        assertEquals(new BigDecimal("48.02"), pv.getF1().stripTrailingZeros());
        assertEquals(new BigDecimal("1.0002"), pv.getF2().stripTrailingZeros());
        assertEquals(new BigDecimal("49.0202"), pv.getF12().stripTrailingZeros());
    }
}

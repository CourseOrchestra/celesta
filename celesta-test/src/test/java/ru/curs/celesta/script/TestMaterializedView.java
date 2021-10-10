package ru.curs.celesta.script;

import mView.MView1Cursor;
import mView.MView2Cursor;
import mView.MView3Cursor;
import mView.MView4Cursor;
import mView.MView5Cursor;
import mView.MView6Cursor;
import mView.MViewReverseOrderCursor;
import mView.Table1Cursor;
import mView.Table2Cursor;
import mView.Table3Cursor;
import mView.Table4Cursor;
import mView.Table5Cursor;
import org.junit.jupiter.api.TestTemplate;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.dbutils.Cursor;
import ru.curs.celesta.dbutils.MaterializedViewCursor;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestMaterializedView implements ScriptTest {
    @TestTemplate
    void test_mat_view_insert(CallContext context) {
        Table1Cursor tableCursor = new Table1Cursor(context);
        MView1Cursor mViewCursor = new MView1Cursor(context);
        _test_mat_view_insert(tableCursor, mViewCursor);
    }

    @TestTemplate
    void test_mat_view_insert_with_no_version_check(CallContext context) {
        Table2Cursor tableCursor = new Table2Cursor(context);
        MView3Cursor mViewCursor = new MView3Cursor(context);
        _test_mat_view_insert(tableCursor, mViewCursor);
    }

    @TestTemplate
    void test_mat_view_update(CallContext context) {
        Table1Cursor tableCursor = new Table1Cursor(context);
        MView1Cursor mViewCursor = new MView1Cursor(context);
        _test_mat_view_update(tableCursor, mViewCursor);
    }

    @TestTemplate
    void test_mat_view_update_with_no_version_check(CallContext context) {
        Table2Cursor tableCursor = new Table2Cursor(context);
        MView3Cursor mViewCursor = new MView3Cursor(context);
        _test_mat_view_update(tableCursor, mViewCursor);
    }

    @TestTemplate
    void test_mat_view_delete(CallContext context) {
        Table1Cursor tableCursor = new Table1Cursor(context);
        MView1Cursor mViewCursor = new MView1Cursor(context);
        _test_mat_view_delete(tableCursor, mViewCursor);
    }

    @TestTemplate
    void test_mat_view_delete_with_no_version_check(CallContext context) {
        Table2Cursor tableCursor = new Table2Cursor(context);
        MView3Cursor mViewCursor = new MView3Cursor(context);
        _test_mat_view_delete(tableCursor, mViewCursor);
    }

    @TestTemplate
    void test_mat_view_two_columns(CallContext context) {
        Table4Cursor tableCursor = new Table4Cursor(context);
        MView5Cursor mViewCursor = new MView5Cursor(context);
        assertEquals(0, mViewCursor.count());
        tableCursor
                .setVar1("A")
                .setVar2("B")
                .setNumb(3)
                .insert();
        tableCursor
                .setId(null)
                .setNumb(2)
                .insert();
        assertEquals(1, mViewCursor.count());
        mViewCursor.get("A", "B");
        assertEquals(5, mViewCursor.getS().intValue());

        tableCursor
                .setId(null)
                .setVar2("C")
                .setNumb(4)
                .insert();
        mViewCursor.get("A", "C");
        assertEquals(4, mViewCursor.getS().intValue());
        mViewCursor.tryGetCurrent();
        assertEquals(4, mViewCursor.getS().intValue());
    }

    /*
        Этот тест необходим для гарантии того, что в materialized view останется результат SUM(), даже если он равен 0.;
    */
    @TestTemplate
    void test_mat_view_update_when_count_is_unknown(CallContext context) {
        Table1Cursor tableCursor = new Table1Cursor(context);
        MView2Cursor mViewCursor = new MView2Cursor(context);

        tableCursor.deleteAll();
        assertEquals(0, mViewCursor.count());

        tableCursor
                .setNumb(5)
                .setVar("A")
                .insert();
        Integer id1 = tableCursor.getId();
        tableCursor.clear();

        tableCursor
                .setNumb(2)
                .setVar("A")
                .insert();
        tableCursor.clear();

        mViewCursor.get("A");
        assertEquals(7, mViewCursor.getS().intValue());

        tableCursor.setRange(tableCursor.COLUMNS.numb(), 2);
        tableCursor.first();
        tableCursor
                .setNumb(-5)
                .update();
        tableCursor.clear();

        mViewCursor.get("A");
        assertEquals(0, mViewCursor.getS().intValue());

        tableCursor
                .setNumb(5)
                .setVar("A")
                .insert();
        tableCursor.clear();

        mViewCursor.get("A");
        assertEquals(5, mViewCursor.getS().intValue());

        tableCursor.get(id1);
        tableCursor
                .setVar("B")
                .update();
        tableCursor.clear();

        mViewCursor.get("A");
        assertEquals(0, mViewCursor.getS().intValue());
        mViewCursor.get("B");
        assertEquals(5, mViewCursor.getS().intValue());
    }

    @TestTemplate
    void test_mat_view_date_rounding(CallContext context) {
        Table3Cursor tableCursor = new Table3Cursor(context);
        MView4Cursor mViewCursor = new MView4Cursor(context);

        tableCursor.deleteAll();
        assertEquals(0, mViewCursor.count());

        LocalDateTime datetime1 = LocalDateTime.of(2000, Month.AUGUST, 5, 10, 5, 32);
        LocalDateTime date1 = datetime1.truncatedTo(ChronoUnit.DAYS);

        tableCursor
                .setNumb(5)
                .setDate(Timestamp.valueOf(datetime1))
                .insert();
        tableCursor.clear();

        LocalDateTime datetime2 = LocalDateTime.of(2000, Month.AUGUST, 5, 22, 5, 32);
        tableCursor
                .setNumb(2)
                .setDate(Timestamp.valueOf(datetime2))
                .insert();
        tableCursor.clear();

        LocalDateTime datetime3 = LocalDateTime.of(2000, Month.AUGUST, 6, 10, 5, 32);
        LocalDateTime date2 = datetime3.truncatedTo(ChronoUnit.DAYS);
        tableCursor
                .setNumb(5)
                .setDate(Timestamp.valueOf(datetime3))
                .insert();
        tableCursor.clear();

        assertEquals(2, mViewCursor.count());
        mViewCursor.get(Timestamp.valueOf(date1));
        assertEquals(7, mViewCursor.getS().intValue());

        mViewCursor.get(Timestamp.valueOf(date2));
        assertEquals(5, mViewCursor.getS().intValue());
    }

    @TestTemplate
    void testSumOfDecimal(CallContext context) {
        Table5Cursor t = new Table5Cursor(context);
        MView6Cursor mv = new MView6Cursor(context);

        t.insert();
        t.clear();
        t.insert();
        t.clear();
        t.setF1(new BigDecimal("24.02"));
        t.insert();

        mv.first();
        assertEquals(new BigDecimal("24.01"), mv.getF1());
        assertEquals(new BigDecimal("48.02"), mv.getS1());
        assertEquals(new BigDecimal("2.0002"), mv.getS2());

        mv.next();
        assertEquals(new BigDecimal("24.02"), mv.getF1());
        assertEquals(new BigDecimal("24.02"), mv.getS1());
        assertEquals(new BigDecimal("1.0001"), mv.getS2());
    }


    void setNumb(Cursor t, int numb) {
        if (t instanceof Table1Cursor) {
            ((Table1Cursor) t).setNumb(numb);
        } else {
            ((Table2Cursor) t).setNumb(numb);
        }
    }

    void setVar(Cursor t, String var) {
        if (t instanceof Table1Cursor) {
            ((Table1Cursor) t).setVar(var);
        } else {
            ((Table2Cursor) t).setVar(var);
        }
    }

    int getId(Cursor t) {
        if (t instanceof Table1Cursor) {
            return ((Table1Cursor) t).getId();
        } else {
            return ((Table2Cursor) t).getId();
        }
    }

    int getS(MaterializedViewCursor m) {
        if (m instanceof MView1Cursor) {
            return ((MView1Cursor) m).getS();
        } else {
            return ((MView3Cursor) m).getS();
        }
    }

    int getC(MaterializedViewCursor m) {
        if (m instanceof MView1Cursor) {
            return ((MView1Cursor) m).getC();
        } else {
            return ((MView3Cursor) m).getC();
        }
    }

    void _test_mat_view_insert(Cursor tableCursor, MaterializedViewCursor mViewCursor) {
        tableCursor.deleteAll();

        setNumb(tableCursor, 5);
        setVar(tableCursor, "A");
        tableCursor.insert();
        tableCursor.clear();

        setNumb(tableCursor, 2);
        setVar(tableCursor, "A");
        tableCursor.insert();
        tableCursor.clear();

        setNumb(tableCursor, 0);
        setVar(tableCursor, "A");
        tableCursor.insert();
        tableCursor.clear();

        setNumb(tableCursor, -1);
        setVar(tableCursor, "A");
        tableCursor.insert();
        tableCursor.clear();

        assertEquals(1, mViewCursor.count());

        setNumb(tableCursor, 20);
        setVar(tableCursor, "B");
        tableCursor.insert();
        tableCursor.clear();

        setNumb(tableCursor, 11);
        setVar(tableCursor, "B");
        tableCursor.insert();
        tableCursor.clear();

        assertEquals(2, mViewCursor.count());

        mViewCursor.getByValuesArray("A");
        assertEquals(6, getS(mViewCursor));
        assertEquals(4, getC(mViewCursor));

        mViewCursor.getByValuesArray("B");
        assertEquals(31, getS(mViewCursor));
        assertEquals(2, getC(mViewCursor));

        mViewCursor.setRange("var", "A");
        assertEquals(1, mViewCursor.count());
        mViewCursor.first();
        assertEquals(6, getS(mViewCursor));
        assertEquals(4, getC(mViewCursor));

        mViewCursor.setRange("var", "B");
        assertEquals(1, mViewCursor.count());
        mViewCursor.first();
        assertEquals(31, getS(mViewCursor));
        assertEquals(2, getC(mViewCursor));
    }

    void _test_mat_view_update(Cursor tableCursor, MaterializedViewCursor mViewCursor) {
        tableCursor.deleteAll();
        assertEquals(0, mViewCursor.count());

        setNumb(tableCursor, 5);
        setVar(tableCursor, "A");
        tableCursor.insert();
        tableCursor.clear();

        setNumb(tableCursor, 2);
        setVar(tableCursor, "A");
        tableCursor.insert();
        tableCursor.clear();

        mViewCursor.getByValuesArray("A");
        assertEquals(7, getS(mViewCursor));

        setNumb(tableCursor, 20);
        setVar(tableCursor, "B");
        tableCursor.insert();
        tableCursor.clear();

        setNumb(tableCursor, 11);
        setVar(tableCursor, "B");
        tableCursor.insert();
        tableCursor.clear();

        tableCursor.setRange("numb", 2);
        tableCursor.first();
        setNumb(tableCursor, 4);
        tableCursor.update();
        tableCursor.clear();

        tableCursor.setRange("numb", 11);
        tableCursor.first();
        setNumb(tableCursor, 15);
        tableCursor.update();
        tableCursor.clear();

        assertEquals(2, mViewCursor.count());

        mViewCursor.getByValuesArray("A");
        assertEquals(9, getS(mViewCursor));
        assertEquals(2, getC(mViewCursor));

        mViewCursor.getByValuesArray("B");
        assertEquals(35, getS(mViewCursor));
        assertEquals(2, getC(mViewCursor));
    }

    void _test_mat_view_delete(Cursor tableCursor, MaterializedViewCursor mViewCursor) {
        tableCursor.deleteAll();

        setNumb(tableCursor, 6);
        setVar(tableCursor, "A");
        tableCursor.insert();
        int old_id = getId(tableCursor);
        tableCursor.clear();

        setNumb(tableCursor, 2);
        setVar(tableCursor, "A");
        tableCursor.insert();
        tableCursor.clear();

        mViewCursor.getByValuesArray("A");
        assertEquals(8, getS(mViewCursor));

        tableCursor.getByValuesArray(old_id);
        tableCursor.delete();
        mViewCursor.getByValuesArray("A");
        assertEquals(2, getS(mViewCursor));

        setNumb(tableCursor, 5);
        setVar(tableCursor, "A");
        tableCursor.insert();
        tableCursor.clear();

        mViewCursor.getByValuesArray("A");
        assertEquals(7, getS(mViewCursor));

        setNumb(tableCursor, 20);
        setVar(tableCursor, "B");
        tableCursor.insert();
        tableCursor.clear();

        setNumb(tableCursor, 11);
        setVar(tableCursor, "B");
        tableCursor.insert();
        tableCursor.clear();

        tableCursor.setRange("numb", 2);
        tableCursor.first();
        tableCursor.delete();
        tableCursor.clear();

        assertEquals(2, mViewCursor.count());

        mViewCursor.getByValuesArray("A");
        assertEquals(5, getS(mViewCursor));
        assertEquals(1, getC(mViewCursor));

        tableCursor.setRange("numb", 11);
        tableCursor.first();
        tableCursor.delete();
        tableCursor.clear();

        mViewCursor.getByValuesArray("B");
        assertEquals(20, getS(mViewCursor));
        assertEquals(1, getC(mViewCursor));

        tableCursor.setRange("var", "A");
        tableCursor.first();
        tableCursor.delete();

        assertEquals(1, mViewCursor.count());
    }

    @TestTemplate
    void test_mat_view_reverse_order_of_columns(CallContext ctx) {
        Table4Cursor t4 = new Table4Cursor(ctx);
        t4.setVar1("v1").setVar2("v2").setNumb(1).insert();
        t4.clear();
        t4.setVar1("v1").setVar2("v2").setNumb(2).insert();
        MViewReverseOrderCursor reverseOrderCursor = new MViewReverseOrderCursor(ctx);
        reverseOrderCursor.get("v2", "v1");
        assertEquals(3, reverseOrderCursor.getS());
    }
}

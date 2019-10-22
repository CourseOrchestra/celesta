package ru.curs.celesta.script;


import org.junit.jupiter.api.TestTemplate;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.DBType;
import testTable.*;

import java.io.*;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.TimeZone;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.jupiter.api.Assertions.*;

public class TestTable implements ScriptTest {

    @TestTemplate
    public void test_calc_blob(CallContext cc) throws IOException {

        TBlobCursor cursor = new TBlobCursor(cc);
        cursor.deleteAll();

        cursor.insert();
        cursor.get(1);

        assertEquals(1, cursor.getId().intValue());
        assertNull(cursor.getDat());

        cursor.calcDat();

        assertNotNull(cursor.getDat());
        assertTrue(cursor.getDat().isNull());

        OutputStream os = cursor.getDat().getOutStream();
        try (OutputStreamWriter osw = new OutputStreamWriter(os, "utf-8")) {
            osw.append("blob field");
        }

        cursor.update();
        cursor.clear();
        cursor.get(1);
        cursor.calcDat();
        try (BufferedReader bf = new BufferedReader(
                new InputStreamReader(cursor.getDat().getInStream(), "utf-8"))) {
            assertEquals("blob field", bf.readLine());
        }

        cursor.clear();
        cursor.calcDat();
        os = cursor.getDat().getOutStream();
        try (OutputStreamWriter osw = new OutputStreamWriter(os, "utf-8")) {
            osw.append("blob field 2!");
        }

        cursor.insert();

        cursor.clear();
        cursor.get(2);
        cursor.calcDat();
        try (BufferedReader bf = new BufferedReader(
                new InputStreamReader(cursor.getDat().getInStream(), "utf-8"))) {
            assertEquals("blob field 2!", bf.readLine());
        }
    }

    @TestTemplate
    public void test_getXRec(CallContext cc) {
        TXRecCursor cursor = new TXRecCursor(cc);
        cursor.deleteAll();

        int id = 1;
        int num = 10;
        double cost = 10.2;
        String title = "product";
        boolean isActive = true;
        Timestamp created = Timestamp.valueOf(LocalDateTime.of(2018, Month.of(1), 11, 19, 15));

        cursor.setNum(num);
        cursor.setCost(cost);
        cursor.setTitle(title);
        cursor.setIsActive(isActive);
        cursor.setCreated(created);

        //TODO: type cast!
        TXRecCursor xRec = (TXRecCursor) cursor.getXRec();
        assertXRecCursorFields(xRec, null, null, null, null, null, null);
        cursor.insert();

        //TODO: WTF? worked in Python, but I cannot see why.
        //@ioanngolovko maybe just ignore this
        assertXRecCursorFields(xRec, id, num, cost, title, isActive, created);
        cursor.clear();

        xRec = (TXRecCursor) cursor.getXRec();
        assertXRecCursorFields(xRec, null, null, null, null, null, null);

        cursor.get(1);
        assertXRecCursorFields(xRec, id, num, cost, title, isActive, created);

        cursor.setNum(num + 1);
        cursor.setCost(cost + 1.0);
        cursor.setTitle(title + "asd");
        cursor.setIsActive(false);
        cursor.setCreated(Timestamp.valueOf(LocalDateTime.of(2017, Month.of(1), 11, 19, 15)));

        assertXRecCursorFields(xRec, id, num, cost, title, isActive, created);
    }

    @TestTemplate
    public void test_asCSVLine(CallContext cc) {
        TCsvLineCursor cursor = new TCsvLineCursor(cc);
        assertEquals("NULL,NULL", cursor.asCSVLine());

        cursor.setId(1);
        assertEquals("1,NULL", cursor.asCSVLine());

        cursor.setTitle("noQuotes");
        assertEquals("1,noQuotes", cursor.asCSVLine());

        cursor.setTitle("\"withQuotes\"");
        assertEquals("1,\"\"\"withQuotes\"\"\"", cursor.asCSVLine());

        cursor.setTitle(null);
        assertEquals("1,NULL", cursor.asCSVLine());
    }

    @TestTemplate
    public void test_iterate(CallContext cc) {
        TIterateCursor cursor = new TIterateCursor(cc);
        cursor.insert();
        cursor.clear();
        cursor.insert();

        ArrayList<Integer> idList = new ArrayList<>();
        for (TIterateCursor c : cursor) {
            idList.add(c.getId());
        }

        assertEquals(2, idList.size());
        assertEquals(1, idList.get(0).intValue());
        assertEquals(2, idList.get(1).intValue());
    }

    @TestTemplate
    public void test_CopyFieldsFrom(CallContext cc) {
        TCopyFieldsCursor cursor = new TCopyFieldsCursor(cc);

        TCopyFieldsCursor cursorFrom = new TCopyFieldsCursor(cc);

        int id = 11234;
        String title = "ttt";

        cursorFrom.setId(id);
        cursorFrom.setTitle(title);
        cursor.copyFieldsFrom(cursorFrom);

        assertEquals(id, cursor.getId().intValue());
        assertEquals(title, cursor.getTitle());
    }

    @TestTemplate
    public void test_limit(CallContext cc) {
        TLimitCursor cursor = new TLimitCursor(cc);

        for (int i = 0; i < 3; i++) {
            cursor.insert();
            cursor.clear();
        }

        ArrayList<Integer> idList = new ArrayList<>();

        cursor.limit(0, 2);
        for (TLimitCursor c : cursor) {
            idList.add(c.getId());
        }

        assertEquals(2, idList.size());
        assertEquals(1, idList.get(0).intValue());
        assertEquals(2, idList.get(1).intValue());
        idList.clear();

        cursor.limit(2, 1);
        for (TLimitCursor c : cursor) {
            idList.add(c.getId());
        }
        assertEquals(1, idList.size());
        assertEquals(3, idList.get(0).intValue());

        idList.clear();


        cursor.limit(3, 0);
        for (TLimitCursor c : cursor) {
            idList.add(c.getId());
        }
        assertEquals(0, idList.size());
        idList.clear();

        cursor.limit(3, 5);
        for (TLimitCursor c : cursor) {
            idList.add(c.getId());
        }
        assertEquals(0, idList.size());
    }

    @TestTemplate
    public void test_decimal(CallContext cc) {
        TWithDecimalCursor c = new TWithDecimalCursor(cc);

        c.insert();
        c.first();
        assertEquals(new BigDecimal("5.2"), c.getCost().stripTrailingZeros());

        c.setCost(new BigDecimal("5.289"));
        c.update();
        c.first();
        assertEquals(new BigDecimal("5.29"), c.getCost().stripTrailingZeros());

        c.setCost(new BigDecimal("123.2"));
        //TODO: known non-conformity
        if (DBType.FIREBIRD.equals(cc.getDbAdaptor().getType())) {
            c.update();
            c.first();
            assertEquals(new BigDecimal("123.2"), c.getCost().stripTrailingZeros());
        } else{
            assertThrows(CelestaException.class, () -> c.update());
        }

        c.setCost(new BigDecimal("1234.25235"));

        if (DBType.FIREBIRD.equals(cc.getDbAdaptor().getType())) {
            c.update();
            c.first();
            assertEquals(new BigDecimal("1234.25"), c.getCost().stripTrailingZeros());
        } else{
            assertThrows(CelestaException.class, () -> c.update());
        }
    }

    @TestTemplate
    public void test_datetime_with_time_zone(CallContext cc) {
        TimeZone oldDefaultTimeZone = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("GMT+4"));
            TWithDateTimeZCursor c = new TWithDateTimeZCursor(cc);

            ZoneId zoneId = ZoneId.of("GMT+2");
            /*
            This is
            the datetime
            we will
            insert*/
            LocalDateTime localDateTime = LocalDateTime.of(2017, Month.DECEMBER, 31, 22, 0, 0);
            ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, zoneId);
            /*
            This is
            the datetime
            we expect
            the database
            to receive*/
            LocalDateTime utcDateTime = LocalDateTime.of(2018, Month.JANUARY, 1, 0, 0, 0);

            c.setEventDate(zonedDateTime);
            c.insert();
            c.clear();

            c.first();

            ZoneId zoneIdAfterSelect = ZoneId.of("GMT+4");
            assertEquals(utcDateTime, c.getEventDate().toLocalDateTime());
            assertEquals(zoneIdAfterSelect, c.getEventDate().getZone());

        } finally {
            TimeZone.setDefault(oldDefaultTimeZone);
        }

    }

    private void assertXRecCursorFields(TXRecCursor cursor, Integer id, Integer num, Double cost, String title, Boolean isActive, Timestamp created) {
        assertEquals(id, cursor.getId());
        assertEquals(num, cursor.getNum());
        assertEquals(cost, cursor.getCost());
        assertEquals(title, cursor.getTitle());
        assertEquals(isActive, cursor.getIsActive());
        assertEquals(created, cursor.getCreated());
    }

    @TestTemplate
    public void test_setRange(CallContext cc) {

        TXRecCursor cursor = new TXRecCursor(cc);
        cursor.deleteAll();

        cursor.setNum(11);
        cursor.insert();

        cursor.clear();
        cursor.setNum(22);
        cursor.insert();

        cursor.clear();
        cursor.setNum(33);
        cursor.insert();

        cursor = new TXRecCursor(cc);
        assertEquals(3, cursor.count());

        cursor.setRange(cursor.COLUMNS.num(), 22);
        assertEquals(1, cursor.count());

        cursor.setRange(cursor.COLUMNS.num());
        assertEquals(3, cursor.count());

        cursor.setRange(cursor.COLUMNS.num(), 22, 33);
        assertEquals(2, cursor.count());
    }

    @TestTemplate
    public void test_setFilter(CallContext cc) {

        TXRecCursor cursor = new TXRecCursor(cc);
        cursor.deleteAll();

        cursor.setNum(11);
        cursor.insert();

        cursor.clear();
        cursor.setNum(22);
        cursor.insert();

        cursor.clear();
        cursor.setNum(33);
        cursor.insert();

        cursor = new TXRecCursor(cc);
        assertEquals(3, cursor.count());

        cursor.setFilter(cursor.COLUMNS.num(), "22");
        assertEquals(1, cursor.count());

        cursor.setRange(cursor.COLUMNS.num());
        assertEquals(3, cursor.count());

        cursor.setFilter(cursor.COLUMNS.num(), "22..33");
        assertEquals(2, cursor.count());
    }

}

# coding=UTF-8


from java.sql import Timestamp
from java.math import BigDecimal
from java.time import LocalDateTime, LocalDate, Month
from java.time.temporal import ChronoUnit
from mView._mView_orm import table1Cursor, table2Cursor, table3Cursor, table4Cursor, table5Cursor,\
    mView1Cursor, mView2Cursor, mView3Cursor, mView4Cursor, mView5Cursor, mView6Cursor

from ru.curs.celesta.unit import TestClass, CelestaTestCase

from java.lang import Thread, System, String, Exception as JavaException
from java.time import LocalDateTime
from ru.curs.celesta import SessionContext
from ru.curs.celesta import CallContext
from ru.curs.celesta import ConnectionPool

@TestClass
class TestMaterializedView(CelestaTestCase):
    def test_mat_view_insert(self):
        tableCursor = table1Cursor(self.context)
        mViewCursor = mView1Cursor(self.context)
        self._test_mat_view_insert(tableCursor, mViewCursor)

    def test_mat_view_insert_with_no_version_check(self):
        tableCursor = table2Cursor(self.context)
        mViewCursor = mView3Cursor(self.context)
        self._test_mat_view_insert(tableCursor, mViewCursor)

    def test_mat_view_update(self):
        tableCursor = table1Cursor(self.context)
        mViewCursor = mView1Cursor(self.context)
        self._test_mat_view_update(tableCursor, mViewCursor)

    def test_mat_view_update_with_no_version_check(self):
        tableCursor = table2Cursor(self.context)
        mViewCursor = mView3Cursor(self.context)
        self._test_mat_view_update(tableCursor, mViewCursor)

    def test_mat_view_delete(self):
        tableCursor = table1Cursor(self.context)
        mViewCursor = mView1Cursor(self.context)
        self._test_mat_view_delete(tableCursor, mViewCursor)

    def test_mat_view_delete_with_no_version_check(self):
        tableCursor = table2Cursor(self.context)
        mViewCursor = mView3Cursor(self.context)
        self._test_mat_view_delete(tableCursor, mViewCursor)

    def test_mat_view_two_columns(self):
        tableCursor = table4Cursor(self.context)
        mViewCursor = mView5Cursor(self.context)
        self.assertEquals(0, mViewCursor.count())
        tableCursor.var1 = "A"
        tableCursor.var2 = "B"
        tableCursor.numb = 3
        tableCursor.insert()
        tableCursor.id = None
        tableCursor.numb = 2
        tableCursor.insert()
        self.assertEquals(1, mViewCursor.count())
        mViewCursor.get("A", "B")
        self.assertEquals(5, mViewCursor.s)

        tableCursor.id = None
        tableCursor.var2 = "C"
        tableCursor.numb = 4
        tableCursor.insert()
        mViewCursor.get("A", "C")
        self.assertEquals(4, mViewCursor.s)
        mViewCursor.tryGetCurrent()
        self.assertEquals(4, mViewCursor.s)

    '''
        Этот тест необходим для гарантии того, что в materialized view останется результат SUM(), даже если он равен 0.
    '''
    def test_mat_view_update_when_count_is_unknown(self):
        tableCursor = table1Cursor(self.context)
        mViewCursor = mView2Cursor(self.context)

        tableCursor.deleteAll()
        self.assertEquals(0, mViewCursor.count())

        tableCursor.numb = 5
        tableCursor.var = "A"
        tableCursor.insert()
        id1 = tableCursor.id
        tableCursor.clear()

        tableCursor.numb = 2
        tableCursor.var = "A"
        tableCursor.insert()
        tableCursor.clear()

        mViewCursor.get("A")
        self.assertEquals(7, mViewCursor.s)

        tableCursor.setRange('numb', 2)
        tableCursor.first()
        tableCursor.numb = -5
        tableCursor.update()
        tableCursor.clear()

        mViewCursor.get("A")
        self.assertEquals(0, mViewCursor.s)

        tableCursor.numb = 5
        tableCursor.var = "A"
        tableCursor.insert()
        tableCursor.clear()

        mViewCursor.get("A")
        self.assertEquals(5, mViewCursor.s)

        tableCursor.get(id1)
        tableCursor.var = "B"
        tableCursor.update()
        tableCursor.clear()

        mViewCursor.get("A")
        self.assertEquals(0, mViewCursor.s)
        mViewCursor.get("B")
        self.assertEquals(5, mViewCursor.s)

    def test_mat_view_date_rounding(self):
        tableCursor = table3Cursor(self.context)
        mViewCursor = mView4Cursor(self.context)

        tableCursor.deleteAll()
        self.assertEquals(0, mViewCursor.count())

        datetime1 = LocalDateTime.of(2000, Month.AUGUST, 5, 10, 5, 32)
        date1 = datetime1.truncatedTo(ChronoUnit.DAYS)

        tableCursor.numb = 5
        tableCursor.date = Timestamp.valueOf(datetime1)
        tableCursor.insert()
        tableCursor.clear()

        datetime2 = LocalDateTime.of(2000, Month.AUGUST, 5, 22, 5, 32)
        tableCursor.numb = 2
        tableCursor.date = Timestamp.valueOf(datetime2)
        tableCursor.insert()
        tableCursor.clear()

        datetime3 = LocalDateTime.of(2000, Month.AUGUST, 6, 10, 5, 32)
        date2 = datetime3.truncatedTo(ChronoUnit.DAYS)
        tableCursor.numb = 5
        tableCursor.date = Timestamp.valueOf(datetime3)
        tableCursor.insert()
        tableCursor.clear()

        self.assertEquals(2, mViewCursor.count())
        mViewCursor.get(Timestamp.valueOf(date1))
        self.assertEquals(7, mViewCursor.s)

        mViewCursor.get(Timestamp.valueOf(date2))
        self.assertEquals(5, mViewCursor.s)

    def testSumOfDecimal(self):
        t = table5Cursor(self.context)
        mv = mView6Cursor(self.context)

        t.insert()
        t.insert()
        t.f1 = BigDecimal('24.02')
        t.insert()

        mv.first()
        self.assertEquals(BigDecimal('24.01'), mv.f1)
        self.assertEquals(BigDecimal('48.02'), mv.s1)
        self.assertEquals(BigDecimal('2.0002'), mv.s2)

        mv.next()
        self.assertEquals(BigDecimal('24.02'), mv.f1)
        self.assertEquals(BigDecimal('24.02'), mv.s1)
        self.assertEquals(BigDecimal('1.0001'), mv.s2)

    def _test_mat_view_insert(self, tableCursor, mViewCursor):
        tableCursor.deleteAll()

        tableCursor.numb = 5
        tableCursor.var = "A"
        tableCursor.insert()
        tableCursor.clear()

        tableCursor.numb = 2
        tableCursor.var = "A"
        tableCursor.insert()
        tableCursor.clear()

        tableCursor.numb = 0
        tableCursor.var = "A"
        tableCursor.insert()
        tableCursor.clear()

        tableCursor.numb = -1
        tableCursor.var = "A"
        tableCursor.insert()
        tableCursor.clear()

        self.assertEquals(1, mViewCursor.count())

        tableCursor.numb = 20
        tableCursor.var = "B"
        tableCursor.insert()
        tableCursor.clear()

        tableCursor.numb = 11
        tableCursor.var = "B"
        tableCursor.insert()
        tableCursor.clear()

        self.assertEquals(2, mViewCursor.count())

        mViewCursor.get("A")
        self.assertEquals(6, mViewCursor.s)
        self.assertEquals(4, mViewCursor.c)

        mViewCursor.get("B")
        self.assertEquals(31, mViewCursor.s)
        self.assertEquals(2, mViewCursor.c)

        mViewCursor.setRange('var', "A")
        self.assertEquals(1, mViewCursor.count())
        mViewCursor.first()
        self.assertEquals(6, mViewCursor.s)
        self.assertEquals(4, mViewCursor.c)

        mViewCursor.setRange('var', "B")
        self.assertEquals(1, mViewCursor.count())
        mViewCursor.first()
        self.assertEquals(31, mViewCursor.s)
        self.assertEquals(2, mViewCursor.c)

    def _test_mat_view_update(self, tableCursor, mViewCursor):
        tableCursor.deleteAll()
        self.assertEquals(0, mViewCursor.count())

        tableCursor.numb = 5
        tableCursor.var = "A"
        tableCursor.insert()
        tableCursor.clear()

        tableCursor.numb = 2
        tableCursor.var = "A"
        tableCursor.insert()
        tableCursor.clear()

        mViewCursor.get("A")
        self.assertEquals(7, mViewCursor.s)

        tableCursor.numb = 20
        tableCursor.var = "B"
        tableCursor.insert()
        tableCursor.clear()

        tableCursor.numb = 11
        tableCursor.var = "B"
        tableCursor.insert()
        tableCursor.clear()

        tableCursor.setRange('numb', 2)
        tableCursor.first()
        tableCursor.numb = 4
        tableCursor.update()
        tableCursor.clear()

        tableCursor.setRange('numb', 11)
        tableCursor.first()
        tableCursor.numb = 15
        tableCursor.update()
        tableCursor.clear()

        self.assertEquals(2, mViewCursor.count())

        mViewCursor.get("A")
        self.assertEquals(9, mViewCursor.s)
        self.assertEquals(2, mViewCursor.c)

        mViewCursor.get("B")
        self.assertEquals(35, mViewCursor.s)
        self.assertEquals(2, mViewCursor.c)

    def _test_mat_view_delete(self, tableCursor, mViewCursor):
        tableCursor.deleteAll()

        tableCursor.numb = 6
        tableCursor.var = "A"
        tableCursor.insert()
        old_id = tableCursor.id
        tableCursor.clear()

        tableCursor.numb = 2
        tableCursor.var = "A"
        tableCursor.insert()
        tableCursor.clear()

        mViewCursor.get("A")
        self.assertEquals(8, mViewCursor.s)

        tableCursor.get(old_id)
        tableCursor.delete()
        mViewCursor.get("A")
        self.assertEquals(2, mViewCursor.s)

        tableCursor.numb = 5
        tableCursor.var = "A"
        tableCursor.insert()
        tableCursor.clear()

        mViewCursor.get("A")
        self.assertEquals(7, mViewCursor.s)

        tableCursor.numb = 20
        tableCursor.var = "B"
        tableCursor.insert()
        tableCursor.clear()

        tableCursor.numb = 11
        tableCursor.var = "B"
        tableCursor.insert()
        tableCursor.clear()

        tableCursor.setRange('numb', 2)
        tableCursor.first()
        tableCursor.delete()
        tableCursor.clear()

        self.assertEquals(2, mViewCursor.count())

        mViewCursor.get("A")
        self.assertEquals(5, mViewCursor.s)
        self.assertEquals(1, mViewCursor.c)

        tableCursor.setRange('numb', 11)
        tableCursor.first()
        tableCursor.delete()
        tableCursor.clear()

        mViewCursor.get("B")
        self.assertEquals(20, mViewCursor.s)
        self.assertEquals(1, mViewCursor.c)

        tableCursor.setRange('var', "A")
        tableCursor.first()
        tableCursor.delete()

        self.assertEquals(1, mViewCursor.count())
        '''

    def testMultiThread(self):
        tableCursor = table1Cursor(self.context)

        tableCursor.deleteAll()

        self.tearDown()

        thread1 = TableWriterThread()
        thread1.setName('TableWriterThread1')
        thread2 = TableWriterThread()
        thread2.setName('TableWriterThread2')
        thread3 = TableWriterThread()
        thread3.setName('TableWriterThread3')
        thread4 = TableWriterThread()
        thread4.setName('TableWriterThread4')
        thread5 = TableWriterThread()
        thread5.setName('TableWriterThread5')

        thread1.start()
        thread2.start()
        thread3.start()
        thread4.start()
        thread5.start()

        thread1.join()
        thread2.join()
        thread3.join()
        thread4.join()
        thread5.join()

        self.setUp()


class TableWriterThread(Thread):
    def run(self):
        try:
            end = LocalDateTime.now().plusMinutes(1)

            tick = 1
            while LocalDateTime.now().isBefore(end):
                sessionContext = SessionContext('super', 'debug')
                conn = ConnectionPool.get()
                context = CallContext(conn, sessionContext)
                tableCursor = table1Cursor(context)

                tableCursor.numb = 6
                tableCursor.var = "A"
                tableCursor.insert()

                context.closeCursors()
                ConnectionPool.putBack(conn)

                System.out.println('insert completed: tick ' + String.valueOf(tick) + ' ===>' + self.getName())

                if self.getName() == "TableWriterThread1":
                    sessionContext = SessionContext('super', 'debug')
                    conn = ConnectionPool.get()
                    context = CallContext(conn, sessionContext)
                    tableCursor = table1Cursor(context)

                    tableCursor.deleteAll()

                    context.closeCursors()
                    ConnectionPool.putBack(conn)
                    System.out.println('delete completed: tick ' + String.valueOf(tick) + ' ===>' + self.getName())

                tick = tick + 1
        except JavaException, e:
            e.printStackTrace()
            raise e
        '''

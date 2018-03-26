# coding=UTF-8

from java.math import BigDecimal

from pView._pView_orm import t1Cursor, t2Cursor, t3Cursor, pView1Cursor, pView2Cursor, pView3Cursor, pView4Cursor
from ru.curs.celesta.unit import TestClass, CelestaTestCase


@TestClass
class TestParameterizedView(CelestaTestCase):

    def test_p_view_with_aggregate(self):
        tCursor = t1Cursor(self.context)
        tCursor.deleteAll()

        tCursor.f1 = 1
        tCursor.f2 = 2
        tCursor.f3 = 'A'
        tCursor.insert()
        tCursor.clear()

        tCursor.f1 = 9
        tCursor.f2 = 2
        tCursor.f3 = 'A'
        tCursor.insert()
        tCursor.clear()

        tCursor.f1 = 4
        tCursor.f2 = 3
        tCursor.f3 = 'A'
        tCursor.insert()
        tCursor.clear()

        tCursor.f1 = 7
        tCursor.f2 = 2
        tCursor.f3 = 'B'
        tCursor.insert()
        tCursor.clear()

        pvCursor = pView1Cursor(self.context, 1)
        self.assertEquals(0, pvCursor.count())

        pvCursor = pView1Cursor(self.context, 2)
        pvCursor.orderBy('f3')
        self.assertEquals(2, pvCursor.count())

        pvCursor.first()
        self.assertEquals(10, pvCursor.sumv)
        self.assertEquals('A', pvCursor.f3)
        pvCursor.next()
        self.assertEquals(7, pvCursor.sumv)
        self.assertEquals('B', pvCursor.f3)

        pvCursor = pView1Cursor(self.context, 3)
        self.assertEquals(1, pvCursor.count())

        pvCursor.first()
        self.assertEquals(4, pvCursor.sumv)
        self.assertEquals('A', pvCursor.f3)

    def test_p_view_with_two_parameters(self):
        tCursor = t1Cursor(self.context)
        tCursor.deleteAll()

        tCursor.f1 = 1
        tCursor.f2 = 2
        tCursor.f3 = 'A'
        tCursor.insert()
        tCursor.clear()

        tCursor.f1 = 9
        tCursor.f2 = 2
        tCursor.f3 = 'A'
        tCursor.insert()
        tCursor.clear()

        tCursor.f1 = 4
        tCursor.f2 = 3
        tCursor.f3 = 'A'
        tCursor.insert()
        tCursor.clear()

        tCursor.f1 = 7
        tCursor.f2 = 2
        tCursor.f3 = 'B'
        tCursor.insert()
        tCursor.clear()

        pvCursor = pView2Cursor(self.context, 2, 'C')
        self.assertEquals(0, pvCursor.count())

        pvCursor = pView2Cursor(self.context, 2, 'A')
        pvCursor.orderBy('f1')
        self.assertEquals(2, pvCursor.count())

        pvCursor.first()
        self.assertEquals(1, pvCursor.f1)
        self.assertEquals(2, pvCursor.f2)
        self.assertEquals('A', pvCursor.f3)
        pvCursor.next()
        self.assertEquals(9, pvCursor.f1)
        self.assertEquals(2, pvCursor.f2)
        self.assertEquals('A', pvCursor.f3)

        pvCursor = pView2Cursor(self.context, 3, 'A')
        self.assertEquals(1, pvCursor.count())

        pvCursor.first()
        self.assertEquals(4, pvCursor.f1)
        self.assertEquals(3, pvCursor.f2)
        self.assertEquals('A', pvCursor.f3)

        pvCursor = pView2Cursor(self.context, 2, 'B')
        self.assertEquals(1, pvCursor.count())

        pvCursor.first()
        self.assertEquals(7, pvCursor.f1)
        self.assertEquals(2, pvCursor.f2)
        self.assertEquals('B', pvCursor.f3)

    def test_p_view_with_join(self):
        tCursor1 = t1Cursor(self.context)
        tCursor1.deleteAll()

        tCursor2 = t2Cursor(self.context)
        tCursor2.deleteAll()

        tCursor1.f1 = 1
        tCursor1.f2 = 2
        tCursor1.f3 = 'A'
        tCursor1.insert()
        tCursor1.clear()

        tCursor1.f1 = 9
        tCursor1.f2 = 2
        tCursor1.f3 = 'A'
        tCursor1.insert()
        tCursor1.clear()

        pvCursor = pView3Cursor(self.context, 2)
        self.assertEquals(1, pvCursor.count())
        pvCursor.first()
        self.assertEquals(2, pvCursor.c)

        tCursor2.ff1 = 9
        tCursor2.ff2 = 3
        tCursor2.ff3 = 'B'
        tCursor2.insert()
        tCursor2.clear()

        self.assertEquals(1, pvCursor.count())
        pvCursor.first()
        self.assertEquals(2, pvCursor.c)

        tCursor2.ff1 = 9
        tCursor2.ff2 = 2
        tCursor2.ff3 = 'B'
        tCursor2.insert()
        tCursor2.clear()

        self.assertEquals(1, pvCursor.count())
        pvCursor.first()
        self.assertEquals(2, pvCursor.c)

        tCursor2.ff1 = 9
        tCursor2.ff2 = 2
        tCursor2.ff3 = 'B'
        tCursor2.insert()
        tCursor2.clear()

        # Проверка на декартово произведение
        self.assertEquals(1, pvCursor.count())
        pvCursor.first()
        self.assertEquals(4, pvCursor.c)

    def testSumOfDecimal(self):
        t = t3Cursor(self.context)

        t.insert()
        t.f2 = BigDecimal('0.0001')
        t.insert()

        pv = pView4Cursor(self.context, BigDecimal('1.00001'))
        pv.first()

        self.assertEquals(BigDecimal('24.01'), pv.f1.stripTrailingZeros())
        self.assertEquals(BigDecimal('1.0001'), pv.f2.stripTrailingZeros())
        self.assertEquals(BigDecimal('25.0101'), pv.f12.stripTrailingZeros())

        pv = pView4Cursor(self.context, BigDecimal('0.00001'))
        pv.first()

        self.assertEquals(BigDecimal('48.02'), pv.f1.stripTrailingZeros())
        self.assertEquals(BigDecimal('1.0002'), pv.f2.stripTrailingZeros())
        self.assertEquals(BigDecimal('49.0202'), pv.f12.stripTrailingZeros())
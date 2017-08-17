# coding=UTF-8

from celestaunit.internal_celesta_unit import CelestaUnit
from pView._pView_orm import t1Cursor, t2Cursor, pView1Cursor, pView2Cursor, pView3Cursor

class TestParameterizedView(CelestaUnit):

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
        self.assertEqual(0, pvCursor.count())

        pvCursor = pView1Cursor(self.context, 2)
        pvCursor.orderBy('f3')
        self.assertEqual(2, pvCursor.count())

        pvCursor.first()
        self.assertEqual(10, pvCursor.sumv)
        self.assertEqual('A', pvCursor.f3)
        pvCursor.next()
        self.assertEqual(7, pvCursor.sumv)
        self.assertEqual('B', pvCursor.f3)

        pvCursor = pView1Cursor(self.context, 3)
        self.assertEqual(1, pvCursor.count())

        pvCursor.first()
        self.assertEqual(4, pvCursor.sumv)
        self.assertEqual('A', pvCursor.f3)

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
        self.assertEqual(0, pvCursor.count())

        pvCursor = pView2Cursor(self.context, 2, 'A')
        pvCursor.orderBy('f1')
        self.assertEqual(2, pvCursor.count())

        pvCursor.first()
        self.assertEqual(1, pvCursor.f1)
        self.assertEqual(2, pvCursor.f2)
        self.assertEqual('A', pvCursor.f3)
        pvCursor.next()
        self.assertEqual(9, pvCursor.f1)
        self.assertEqual(2, pvCursor.f2)
        self.assertEqual('A', pvCursor.f3)

        pvCursor = pView2Cursor(self.context, 3, 'A')
        self.assertEqual(1, pvCursor.count())

        pvCursor.first()
        self.assertEqual(4, pvCursor.f1)
        self.assertEqual(3, pvCursor.f2)
        self.assertEqual('A', pvCursor.f3)

        pvCursor = pView2Cursor(self.context, 2, 'B')
        self.assertEqual(1, pvCursor.count())

        pvCursor.first()
        self.assertEqual(7, pvCursor.f1)
        self.assertEqual(2, pvCursor.f2)
        self.assertEqual('B', pvCursor.f3)

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
        self.assertEqual(1, pvCursor.count())
        pvCursor.first()
        self.assertEqual(2, pvCursor.c)

        tCursor2.ff1 = 9
        tCursor2.ff2 = 3
        tCursor2.ff3 = 'B'
        tCursor2.insert()
        tCursor2.clear()

        self.assertEqual(1, pvCursor.count())
        pvCursor.first()
        self.assertEqual(2, pvCursor.c)

        tCursor2.ff1 = 9
        tCursor2.ff2 = 2
        tCursor2.ff3 = 'B'
        tCursor2.insert()
        tCursor2.clear()

        self.assertEqual(1, pvCursor.count())
        pvCursor.first()
        self.assertEqual(2, pvCursor.c)

        tCursor2.ff1 = 9
        tCursor2.ff2 = 2
        tCursor2.ff3 = 'B'
        tCursor2.insert()
        tCursor2.clear()

        # Проверка на декартово произведение
        self.assertEqual(1, pvCursor.count())
        pvCursor.first()
        self.assertEqual(4, pvCursor.c)
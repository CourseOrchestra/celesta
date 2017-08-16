# coding=UTF-8

from celestaunit.internal_celesta_unit import CelestaUnit
from pView._pView_orm import t1Cursor, pView1Cursor, pView2Cursor

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


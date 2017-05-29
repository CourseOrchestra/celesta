# coding=UTF-8

from java.sql import Timestamp
from java.time import LocalDateTime

from celestaunit.internal_celesta_unit import CelestaUnit
from simpleCases._simpleCases_orm import tableForGetDateInViewCursor, viewWithGetDateCursor, zeroInsertCursor

class TestSimpleCases(CelestaUnit):
    def test_getdate_in_view(self):
        viewCursor = viewWithGetDateCursor(self.context)
        self.assertEqual(0, viewCursor.count())

        tableCursor = tableForGetDateInViewCursor(self.context)

        tableCursor.date = Timestamp.valueOf(LocalDateTime.now().minusDays(1))
        tableCursor.insert()
        self.assertEqual(0, viewCursor.count())

        tableCursor.clear()
        tableCursor.date = Timestamp.valueOf(LocalDateTime.now().plusDays(1))
        tableCursor.insert()
        self.assertEqual(1, viewCursor.count())


    def test_zero_insert(self):
        c = zeroInsertCursor(self.context)
        c.insert()
        print c.id
        print c.date



# coding=UTF-8

from java.sql import Timestamp
from java.time import LocalDateTime

from celestaunit.internal_celesta_unit import CelestaUnit
from ztest._ztest_orm import tableForGetDateInViewCursor, viewWithGetDateCursor, zeroInsertCursor
from ztest._ztest_orm import tableCountWithoutConditionCursor, tableCountAndGetDateConditionCursor, viewCountWithoutConditionCursor, viewCountAndGetDateConditionCursor

class TestGetDate(CelestaUnit):
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


    def test_count_without_condition(self):
        viewCursor = viewCountWithoutConditionCursor(self.context)
        self.assertEqual(1, viewCursor.count())
        viewCursor.first()
        self.assertEqual(0, viewCursor.c)

        tableCursor = tableCountWithoutConditionCursor(self.context)
        tableCursor.insert()
        tableCursor.clear()
        tableCursor.insert()

        self.assertEqual(1, viewCursor.count())
        viewCursor.first()
        self.assertEqual(2, viewCursor.c)


    def test_count_with_getdate_condition(self):
        viewCursor = viewCountAndGetDateConditionCursor(self.context)
        viewCursor.first()
        self.assertEqual(0, viewCursor.c)

        tableCursor = tableCountAndGetDateConditionCursor(self.context)
        tableCursor.insert()
        tableCursor.clear()
        tableCursor.date = Timestamp.valueOf(LocalDateTime.now())
        tableCursor.insert()

        viewCursor.first()
        self.assertEqual(0, viewCursor.c)

        tableCursor.clear()
        tableCursor.date = Timestamp.valueOf(LocalDateTime.now().plusDays(1))
        tableCursor.insert()

        viewCursor.first()
        self.assertEqual(1, viewCursor.c)

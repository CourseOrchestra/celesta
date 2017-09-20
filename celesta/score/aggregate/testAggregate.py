# coding=UTF-8

from java.sql import Timestamp
from java.time import LocalDateTime

from aggregate._aggregate_orm import countConditionLessCursor, countGetDateCondCursor\
    , viewCountCondLessCursor, viewCountGetDateCondCursor, tableSumOneFieldCursor\
    , viewSumOneFieldCursor, sumFieldAndNumberCursor, viewSumTwoNumbersCursor\
    , tableSumTwoFieldsCursor, viewSumTwoFieldsCursor, tableMinMaxCursor\
    , viewMinOneFieldCursor, viewMaxOneFieldCursor, viewMinTwoFieldsCursor, viewMaxTwoFieldsCursor\
    , tableGroupByCursor, viewGroupByAggregateCursor, viewGroupByCursor, viewCountMinMaxCursor
from ru.curs.celesta.unit import TestClass, CelestaTestCase


@TestClass
class TestAggregate(CelestaTestCase):

    def test_count_without_condition(self):
        tableCursor = countConditionLessCursor(self.context)
        tableCursor.deleteAll()

        viewCursor = viewCountCondLessCursor(self.context)
        self.assertEquals(1, viewCursor.count())
        viewCursor.first()
        self.assertEquals(0, viewCursor.c)

        tableCursor.insert()
        tableCursor.clear()
        tableCursor.insert()

        self.assertEquals(1, viewCursor.count())
        viewCursor.first()
        self.assertEquals(2, viewCursor.c)


    def test_count_with_getdate_condition(self):
        tableCursor = countGetDateCondCursor(self.context)
        tableCursor.deleteAll()

        viewCursor = viewCountGetDateCondCursor(self.context)
        viewCursor.first()
        self.assertEquals(0, viewCursor.c)

        tableCursor.insert()
        tableCursor.clear()
        tableCursor.date = Timestamp.valueOf(LocalDateTime.now().minusSeconds(1))
        tableCursor.insert()

        viewCursor.first()
        self.assertEquals(0, viewCursor.c)

        tableCursor.clear()
        tableCursor.date = Timestamp.valueOf(LocalDateTime.now().plusDays(1))
        tableCursor.insert()

        viewCursor.first()
        self.assertEquals(1, viewCursor.c)


    def test_sum_one_field(self):
        tableOneFieldCursor = tableSumOneFieldCursor(self.context)
        tableOneFieldCursor.deleteAll()

        viewOneFieldCursor = viewSumOneFieldCursor(self.context)
        viewOneFieldAndNumberCursor= sumFieldAndNumberCursor(self.context)
        viewTwoNumbersCursor = viewSumTwoNumbersCursor(self.context)

        self.assertEquals(1, viewOneFieldCursor.count())
        self.assertEquals(1, viewOneFieldAndNumberCursor.count())
        self.assertEquals(1, viewTwoNumbersCursor.count())

        viewOneFieldCursor.first()
        viewOneFieldAndNumberCursor.first()
        viewTwoNumbersCursor.first()
        self.assertEquals(None, viewOneFieldCursor.s)
        self.assertEquals(None, viewOneFieldAndNumberCursor.s)
        self.assertEquals(None, viewTwoNumbersCursor.s)

        tableOneFieldCursor.f = 4
        tableOneFieldCursor.insert()

        viewOneFieldCursor.first()
        viewOneFieldAndNumberCursor.first()
        viewTwoNumbersCursor.first()
        self.assertEquals(4, viewOneFieldCursor.s)
        self.assertEquals(5, viewOneFieldAndNumberCursor.s)
        self.assertEquals(3, viewTwoNumbersCursor.s)


    def test_sum_two_fields(self):
        tableTwoFieldsCursor = tableSumTwoFieldsCursor(self.context)
        tableTwoFieldsCursor.deleteAll()

        viewTwoFieldsCursor = viewSumTwoFieldsCursor(self.context)

        self.assertEquals(1, viewTwoFieldsCursor.count())

        viewTwoFieldsCursor.first()
        self.assertEquals(None, viewTwoFieldsCursor.s)

        tableTwoFieldsCursor.f1 = 2
        tableTwoFieldsCursor.insert()
        tableTwoFieldsCursor.clear()

        viewTwoFieldsCursor.first()
        self.assertEquals(None, viewTwoFieldsCursor.s)

        tableTwoFieldsCursor.f2 = 2
        tableTwoFieldsCursor.insert()
        tableTwoFieldsCursor.clear()

        viewTwoFieldsCursor.first()
        self.assertEquals(None, viewTwoFieldsCursor.s)

        tableTwoFieldsCursor.f1 = 2
        tableTwoFieldsCursor.f2 = 3
        tableTwoFieldsCursor.insert()
        tableTwoFieldsCursor.clear()

        viewTwoFieldsCursor.first()
        self.assertEquals(5, viewTwoFieldsCursor.s)


    def test_min_and_max_one_field(self):
      tableCur = tableMinMaxCursor(self.context)
      tableCur.deleteAll()

      viewMinOneFieldCur = viewMinOneFieldCursor(self.context)
      viewMaxOneFieldCur = viewMaxOneFieldCursor(self.context)
      viewMinTwoFieldsCur = viewMinTwoFieldsCursor(self.context)
      viewMaxTwoFieldsCur = viewMaxTwoFieldsCursor(self.context)
      viewCountMinMaxCur = viewCountMinMaxCursor(self.context)

      viewMinOneFieldCur.first()
      viewMaxOneFieldCur.first()
      viewMinTwoFieldsCur.first()
      viewMaxTwoFieldsCur.first()
      viewCountMinMaxCur.first()
      self.assertEquals(1, viewMinOneFieldCur.count())
      self.assertEquals(1, viewMaxOneFieldCur.count())
      self.assertEquals(1, viewMinTwoFieldsCur.count())
      self.assertEquals(1, viewMaxTwoFieldsCur.count())
      self.assertEquals(1, viewCountMinMaxCur.count())
      self.assertEquals(None, viewMinOneFieldCur.m)
      self.assertEquals(None, viewMaxOneFieldCur.m)
      self.assertEquals(None, viewMinTwoFieldsCur.m)
      self.assertEquals(None, viewMaxTwoFieldsCur.m)
      self.assertEquals(0, viewCountMinMaxCur.countv)
      self.assertEquals(None, viewCountMinMaxCur.maxv)
      self.assertEquals(None, viewCountMinMaxCur.minv)

      tableCur.f1 = 1
      tableCur.f2 = 5
      tableCur.insert()
      tableCur.clear()

      tableCur.f1 = 5
      tableCur.f2 = 2
      tableCur.insert()
      tableCur.clear()

      viewMinOneFieldCur.first()
      viewMaxOneFieldCur.first()
      viewMinTwoFieldsCur.first()
      viewMaxTwoFieldsCur.first()
      viewCountMinMaxCur.first()

      self.assertEquals(1, viewMinOneFieldCur.m)
      self.assertEquals(5, viewMaxOneFieldCur.m)
      self.assertEquals(6, viewMinTwoFieldsCur.m)
      self.assertEquals(7, viewMaxTwoFieldsCur.m)
      self.assertEquals(2, viewCountMinMaxCur.countv)
      self.assertEquals(5, viewCountMinMaxCur.maxv)
      self.assertEquals(2, viewCountMinMaxCur.minv)


    def testGroupBy(self):
        tableCursor = tableGroupByCursor(self.context)
        tableCursor.deleteAll()

        viewGroupByCur = viewGroupByCursor(self.context)
        viewAggregateCursor = viewGroupByAggregateCursor(self.context)

        self.assertEquals(0, viewAggregateCursor.count())

        name1 = "A"
        name2 = "B"

        tableCursor.name = name1
        tableCursor.cost = 100
        tableCursor.insert()
        tableCursor.clear()

        viewGroupByCur.first()
        self.assertEquals(name1, viewGroupByCur.name)
        self.assertEquals(100, viewGroupByCur.cost)

        tableCursor.name = name1
        tableCursor.cost = 150
        tableCursor.insert()
        tableCursor.clear()
        tableCursor.name = name2
        tableCursor.cost = 50
        tableCursor.insert()
        tableCursor.clear()

        self.assertEquals(2, viewAggregateCursor.count())
        viewAggregateCursor.first()
        self.assertEquals(name1, viewAggregateCursor.name)
        self.assertEquals(250, viewAggregateCursor.s)

        viewAggregateCursor.next()
        self.assertEquals(name2, viewAggregateCursor.name)
        self.assertEquals(50, viewAggregateCursor.s)

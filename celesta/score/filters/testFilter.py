from celestaunit.internal_celesta_unit import CelestaUnit

from java.time import LocalDateTime
from java.sql import Timestamp
from ru.curs.celesta.dbutils.filter.value import FieldsLookup
from ru.curs.celesta import CelestaException
from ru.curs.celesta.score import ParseException
from _filters_orm import aFilterCursor, bFilterCursor


class testFilters(CelestaUnit):

    def testInFilter(self):
        a = aFilterCursor(self.context)
        b = bFilterCursor(self.context)

        a.deleteAll()
        b.deleteAll()

        timestamp = Timestamp.valueOf(LocalDateTime.now())

        a.date = timestamp
        a.number1 = 5
        a.number2 = -10
        a.insert()
        a.clear()

        a.date = timestamp
        a.number1 = 1
        a.number2 = -20
        a.insert()
        a.clear()

        a.date = Timestamp.valueOf(LocalDateTime.now().plusDays(1))
        a.number2 = -30
        a.insert()
        a.clear()

        b.created = timestamp
        b.numb1 = 2
        b.numb2 = -40
        b.insert()
        b.clear()

        b.created = timestamp
        b.numb1 = 5
        b.numb2 = -50
        b.insert()
        b.clear()

        lookup = FieldsLookup(a, b).add("date", "created")
        a.setIn(lookup)
        self.assertEqual(2, a.count())

        lookup = FieldsLookup(a, b).add("date", "created").add("number1", "numb1")
        a.setIn(lookup)
        self.assertEqual(1, a.count())

        lookup = FieldsLookup(a, b).add("date", "created").add("number1", "numb1").add("number2", "numb2")
        a.setIn(lookup)
        self.assertEqual(0, a.count())

    def testInFilterWithRangeInMainCursor(self):
        a = aFilterCursor(self.context)
        b = bFilterCursor(self.context)

        a.deleteAll()
        b.deleteAll()

        timestamp = Timestamp.valueOf(LocalDateTime.now())

        a.date = timestamp
        a.number1 = 5
        a.number2 = -10
        a.insert()
        a.clear()

        a.date = timestamp
        a.number1 = 1
        a.number2 = -20
        a.insert()
        a.clear()

        a.date = Timestamp.valueOf(LocalDateTime.now().plusDays(1))
        a.number2 = -30
        a.insert()
        a.clear()

        b.created = timestamp
        b.numb1 = 2
        b.numb2 = -40
        b.insert()
        b.clear()

        b.created = timestamp
        b.numb1 = 5
        b.numb2 = -50
        b.insert()
        b.clear()

        a.setRange('number1', 5)
        lookup = FieldsLookup(a, b).add("date", "created")
        a.setIn(lookup)
        self.assertEqual(1, a.count())
        a.first()

    def testInFilterWithRangeInOtherCursorBeforeSetIn(self):
        a = aFilterCursor(self.context)
        b = bFilterCursor(self.context)

        a.deleteAll()
        b.deleteAll()

        timestamp = Timestamp.valueOf(LocalDateTime.now())

        self._fillTablesForTestInFilterWithRangeOnOtherCursor(a, b, timestamp)

        b.setRange('numb2', -40)
        lookup = FieldsLookup(a, b).add("date", "created").add("number1", "numb1")
        a.setIn(lookup)

        self.assertEqual(2, a.count())

        a.first()
        self.assertEqual(timestamp, a.date)
        self.assertEqual(5, a.number1)
        self.assertEqual(-10, a.number2)

        a.navigate('>')
        self.assertEqual(timestamp, a.date)
        self.assertEqual(6, a.number1)
        self.assertEqual(-20, a.number2)


    def testInFilterWithRangeInOtherCursorAfterSetIn(self):
        a = aFilterCursor(self.context)
        b = bFilterCursor(self.context)

        a.deleteAll()
        b.deleteAll()

        timestamp = Timestamp.valueOf(LocalDateTime.now())

        self._fillTablesForTestInFilterWithRangeOnOtherCursor(a, b, timestamp)


        lookup = FieldsLookup(a, b).add("date", "created").add("number1", "numb1")
        a.setIn(lookup)
        self.assertEqual(3, a.count())

        b.setRange('numb2', -40)
        self.assertEqual(2, a.count())

        a.first()
        self.assertEqual(timestamp, a.date)
        self.assertEqual(5, a.number1)
        self.assertEqual(-10, a.number2)

        a.navigate('>')
        self.assertEqual(timestamp, a.date)
        self.assertEqual(6, a.number1)
        self.assertEqual(-20, a.number2)


    def testExceptionWhileAddingNotExistedFieldsToLookup(self):
        a = aFilterCursor(self.context)
        b = bFilterCursor(self.context)

        lookup = FieldsLookup(a, b)

        with self.assertRaises(ParseException):
            lookup.add("notExistingField", "created")

        with self.assertRaises(ParseException):
            lookup.add("date", "notExistingField")

        with self.assertRaises(ParseException):
            lookup.add("notExistingField", "notExistingField")



    def testExceptionWhileAddingFieldsWithNotMatchesTypesToLookup(self):
        a = aFilterCursor(self.context)
        b = bFilterCursor(self.context)

        lookup = FieldsLookup(a, b)

        with self.assertRaises(CelestaException) as context:
            lookup.add("date", "numb1")

        self.assertTrue(isinstance(context.exception, CelestaException))


    def testExceptionWhileAddingFieldsWithoutIndexToLookup(self):
        a = aFilterCursor(self.context)
        b = bFilterCursor(self.context)

        lookup = FieldsLookup(a, b)

        with self.assertRaises(CelestaException):
            lookup.add("noIndexA", "numb1")

        with self.assertRaises(CelestaException):
            lookup.add("number1", "noIndexB")

        with self.assertRaises(CelestaException):
            lookup.add("noIndexA", "noIndexB")


    def testExceptionWhenPairsFromLookupDoNotMatchToIndices(self):
        a = aFilterCursor(self.context)
        b = bFilterCursor(self.context)

        lookup = FieldsLookup(a, b)

        with self.assertRaises(CelestaException):
            lookup.add("number1", "numb2")

        with self.assertRaises(CelestaException):
            lookup.add("number2", "numb1")

        lookup.add("date", "created")
        lookup.add("number2", "numb2")
        with self.assertRaises(CelestaException):
            a.setIn(lookup)


    def _fillTablesForTestInFilterWithRangeOnOtherCursor(self, a, b, timestamp):
        a.date = timestamp
        a.number1 = 5
        a.number2 = -10
        a.insert()
        a.clear()

        a.date = timestamp
        a.number1 = 6
        a.number2 = -20
        a.insert()
        a.clear()

        a.date = timestamp
        a.number1 = 1
        a.number2 = -20
        a.insert()
        a.clear()

        a.date = Timestamp.valueOf(LocalDateTime.now().plusDays(1))
        a.number2 = -30
        a.insert()
        a.clear()

        b.created = timestamp
        b.numb1 = 6
        b.numb2 = -40
        b.insert()
        b.clear()

        b.created = timestamp
        b.numb1 = 5
        b.numb2 = -40
        b.insert()
        b.clear()

        b.created = timestamp
        b.numb1 = 1
        b.numb2 = -41
        b.insert()
        b.clear()
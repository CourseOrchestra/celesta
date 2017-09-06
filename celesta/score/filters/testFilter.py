from celestaunit.internal_celesta_unit import CelestaUnit

from java.time import LocalDateTime
from java.sql import Timestamp
from ru.curs.celesta import CelestaException
from ru.curs.celesta.score import ParseException
from _filters_orm import aFilterCursor, bFilterCursor, cFilterCursor, \
    dFilterCursor, eFilterCursor, fFilterCursor, gFilterCursor


class testFilters(CelestaUnit):

    def testInFilterForIndices(self):
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

        lookup = a.setIn(b).add("date", "created");
        self.assertEqual(2, a.count())

        lookup = a.setIn(b).add("date", "created").add("number1", "numb1")
        self.assertEqual(1, a.count())

        a.setIn(b).add("date", "created").add("number1", "numb1").add("number2", "numb2")
        self.assertEqual(0, a.count())


    def testInFilterForSimplePks(self):
        c = cFilterCursor(self.context)
        d = dFilterCursor(self.context)

        c.deleteAll()
        d.deleteAll()

        c.id = 1
        c.insert()
        c.clear()

        c.id = 2
        c.insert()
        c.clear()

        c.id = 3
        c.insert()
        c.clear()

        d.id = 1
        d.insert()
        d.clear()

        d.id = 3
        d.insert()
        d.clear()

        lookup = c.setIn(d).add("id", "id")
        self.assertEqual(2, c.count())


    def testInFilterForComplexPks(self):
        e = eFilterCursor(self.context)
        f = fFilterCursor(self.context)

        e.deleteAll()
        f.deleteAll()

        e.id = 1
        e.number = 1
        e.str = "A"
        e.insert()
        e.clear()

        e.id = 1
        e.number = 1
        e.str = "B"
        e.insert()
        e.clear()

        e.id = 1
        e.number = 3
        e.str = "B"
        e.insert()
        e.clear()

        f.id = 1
        f.numb = 1
        f.insert()
        f.clear()

        lookup = e.setIn(f).add("id", "id").add('number', 'numb')
        self.assertEqual(2, e.count())


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
        lookup = a.setIn(b).add("date", "created")
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
        lookup = a.setIn(b).add("date", "created").add("number1", "numb1")

        self.assertEqual(2, a.count())

        a.first()
        self.assertEqual(5, a.number1)
        self.assertEqual(-10, a.number2)

        a.navigate('>')
        self.assertEqual(6, a.number1)
        self.assertEqual(-20, a.number2)


    def testInFilterWithRangeInOtherCursorAfterSetIn(self):
        a = aFilterCursor(self.context)
        b = bFilterCursor(self.context)

        a.deleteAll()
        b.deleteAll()

        timestamp = Timestamp.valueOf(LocalDateTime.now())

        self._fillTablesForTestInFilterWithRangeOnOtherCursor(a, b, timestamp)


        lookup = a.setIn(b).add("date", "created").add("number1", "numb1")

        self.assertEqual(3, a.count())

        b.setRange('numb2', -40)
        self.assertEqual(2, a.count())

        a.first()
        self.assertEqual(5, a.number1)
        self.assertEqual(-10, a.number2)

        a.navigate('>')
        self.assertEqual(6, a.number1)
        self.assertEqual(-20, a.number2)

    def testInFilterWithAdditionalLookup(self):
        a = aFilterCursor(self.context)
        b = bFilterCursor(self.context)
        g = gFilterCursor(self.context)

        a.deleteAll()
        b.deleteAll()
        g.deleteAll()

        timestamp = Timestamp.valueOf(LocalDateTime.now())

        self._fillTablesForTestInFilterWithRangeOnOtherCursor(a, b, timestamp)

        g.createDate = timestamp
        g.num1 = 5
        g.num2 = -30
        g.insert()
        g.clear()

        g.createDate = timestamp
        g.num1 = 6
        g.num2 = -40
        g.insert()
        g.clear()

        g.createDate = timestamp
        g.num1 = 1
        g.num2 = -41
        g.insert()
        g.clear()

        g.createDate = timestamp
        g.num1 = 1
        g.num2 = -42
        g.insert()
        g.clear()


        lookup = a.setIn(b).add("date", "created").add("number1", "numb1")
        additionalLookup = lookup.and(g).add("date", "createDate").add("number1", "num1")

        self.assertEqual(3, a.count())

        b.setRange('numb2', -40)
        self.assertEqual(2, a.count())

        a.first()
        self.assertEqual(5, a.number1)
        self.assertEqual(-10, a.number2)

        a.navigate('>')
        self.assertEqual(6, a.number1)
        self.assertEqual(-20, a.number2)

        g.setRange('num2', -30)
        self.assertEqual(1, a.count())

        a.first()
        self.assertEqual(5, a.number1)
        self.assertEqual(-10, a.number2)

    def testExceptionWhileAddingNotExistedFieldsToLookup(self):
        a = aFilterCursor(self.context)
        b = bFilterCursor(self.context)

        lookup = a.setIn(b)

        with self.assertRaises(ParseException):
            lookup.add("notExistingField", "created")

        with self.assertRaises(ParseException):
            lookup.add("date", "notExistingField")

        with self.assertRaises(ParseException):
            lookup.add("notExistingField", "notExistingField")



    def testExceptionWhileAddingFieldsWithNotMatchesTypesToLookup(self):
        a = aFilterCursor(self.context)
        b = bFilterCursor(self.context)

        lookup = a.setIn(b)

        with self.assertRaises(CelestaException) as context:
            lookup.add("date", "numb1")

        self.assertTrue(isinstance(context.exception, CelestaException))


    def testExceptionWhileAddingFieldsWithoutIndexToLookup(self):
        a = aFilterCursor(self.context)
        b = bFilterCursor(self.context)

        lookup = a.setIn(b)

        with self.assertRaises(CelestaException):
            lookup.add("noIndexA", "numb1")

        with self.assertRaises(CelestaException):
            lookup.add("number1", "noIndexB")

        with self.assertRaises(CelestaException):
            lookup.add("noIndexA", "noIndexB")


    def testExceptionWhenPairsFromLookupDoNotMatchToIndices(self):
        a = aFilterCursor(self.context)
        b = bFilterCursor(self.context)

        lookup = a.setIn(b)

        with self.assertRaises(CelestaException):
            lookup.add("number1", "numb2")

        with self.assertRaises(CelestaException):
            lookup.add("number2", "numb1")

        with self.assertRaises(CelestaException):
            lookup.add("date", "created")
            lookup.add("number2", "numb2")


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
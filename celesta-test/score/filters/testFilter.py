from java.time import LocalDateTime
from java.sql import Timestamp
from ru.curs.celesta import CelestaException
from ru.curs.celesta.score import ParseException
from _filters_orm import aFilterCursor, bFilterCursor, cFilterCursor, \
    dFilterCursor, eFilterCursor, fFilterCursor, gFilterCursor, hFilterCursor, iFilterCursor
from ru.curs.celesta.unit import TestClass, CelestaTestCase


@TestClass
class TestFilters(CelestaTestCase):

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

        lookup = a.setIn(b).add("date", "created")
        self.assertEquals(2, a.count())

        lookup = a.setIn(b).add("date", "created").add("number1", "numb1")
        self.assertEquals(1, a.count())

        a.setIn(b).add("date", "created").add("number1", "numb1").add("number2", "numb2")
        self.assertEquals(0, a.count())


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
        self.assertEquals(2, c.count())


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
        self.assertEquals(2, e.count())


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
        self.assertEquals(1, a.count())
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

        self.assertEquals(2, a.count())

        a.first()
        self.assertEquals(5, a.number1)
        self.assertEquals(-10, a.number2)

        a.navigate('>')
        self.assertEquals(6, a.number1)
        self.assertEquals(-20, a.number2)


    def testInFilterWithRangeInOtherCursorAfterSetIn(self):
        a = aFilterCursor(self.context)
        b = bFilterCursor(self.context)

        a.deleteAll()
        b.deleteAll()

        timestamp = Timestamp.valueOf(LocalDateTime.now())

        self._fillTablesForTestInFilterWithRangeOnOtherCursor(a, b, timestamp)


        lookup = a.setIn(b).add("date", "created").add("number1", "numb1")

        self.assertEquals(3, a.count())

        b.setRange('numb2', -40)
        self.assertEquals(2, a.count())

        a.first()
        self.assertEquals(5, a.number1)
        self.assertEquals(-10, a.number2)

        a.navigate('>')
        self.assertEquals(6, a.number1)
        self.assertEquals(-20, a.number2)

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

        self.assertEquals(3, a.count())

        b.setRange('numb2', -40)
        self.assertEquals(2, a.count())

        a.first()
        self.assertEquals(5, a.number1)
        self.assertEquals(-10, a.number2)

        a.navigate('>')
        self.assertEquals(6, a.number1)
        self.assertEquals(-20, a.number2)

        g.setRange('num2', -30)
        self.assertEquals(1, a.count())

        a.first()
        self.assertEquals(5, a.number1)
        self.assertEquals(-10, a.number2)


    def testInFilterWhenTargetHasPkAndOtherHasPkWithNotSameOrderAndIndexWithSameOrder(self):
        h = hFilterCursor(self.context)
        i = iFilterCursor(self.context)

        h.deleteAll()
        i.deleteAll()

        h.id = 'H1'
        h.insert()
        h.clear()

        h.id = 'H2'
        h.insert()
        h.clear()

        i.id = 'I1'
        i.hFilterId = 'H1'
        i.insert()
        i.clear()

        lookup = h.setIn(i).add('id', 'hFilterId')
        self.assertEquals(1, h.count())

    def testExceptionWhileAddingNotExistedFieldsToLookup(self):
        a = aFilterCursor(self.context)
        b = bFilterCursor(self.context)

        lookup = a.setIn(b)

        lookupAdd = lambda targetCol, auxiliaryCol : lookup.add(targetCol, auxiliaryCol)

        self.assertThrows(ParseException, lookupAdd, "notExistingField", "created")
        self.assertThrows(ParseException, lookupAdd, "date", "notExistingField")
        self.assertThrows(ParseException, lookupAdd, "notExistingField", "notExistingField")



    def testExceptionWhileAddingFieldsWithNotMatchesTypesToLookup(self):
        a = aFilterCursor(self.context)
        b = bFilterCursor(self.context)

        lookup = a.setIn(b)

        lookupAdd = lambda targetCol, auxiliaryCol : lookup.add(targetCol, auxiliaryCol)
        self.assertThrows(CelestaException, lookupAdd, "date", "numb1")


    def testExceptionWhileAddingFieldsWithoutIndexToLookup(self):
        a = aFilterCursor(self.context)
        b = bFilterCursor(self.context)

        lookup = a.setIn(b)

        lookupAdd = lambda targetCol, auxiliaryCol : lookup.add(targetCol, auxiliaryCol)
        self.assertThrows(CelestaException, lookupAdd, "noIndexA", "numb1")
        self.assertThrows(CelestaException, lookupAdd, "number1", "noIndexB")
        self.assertThrows(CelestaException, lookupAdd, "noIndexA", "noIndexB")


    def testExceptionWhenPairsFromLookupDoNotMatchToIndices(self):
        a = aFilterCursor(self.context)
        b = bFilterCursor(self.context)

        lookup = a.setIn(b)

        lookupAdd = lambda targetCol, auxiliaryCol : lookup.add(targetCol, auxiliaryCol)
        self.assertThrows(CelestaException, lookupAdd, "number1", "numb2")
        self.assertThrows(CelestaException, lookupAdd, "number2", "numb1")

        lookupDoubleAdd = lambda targetCol1, auxiliaryCol1, targetCol2, auxiliaryCol2 : \
            lookup.add(targetCol1, auxiliaryCol1).add(targetCol2, auxiliaryCol2)
        self.assertThrows(CelestaException, lookupDoubleAdd, "date", "created", "number2", "numb2")


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
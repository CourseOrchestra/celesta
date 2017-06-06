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
        a.description = "name1"
        a.number = 5
        a.insert()
        a.clear()

        a.date = timestamp
        a.description = "name2"
        a.number = 1
        a.insert()
        a.clear()

        a.date = Timestamp.valueOf(LocalDateTime.now().plusDays(1))
        a.description = "name3"
        a.insert()
        a.clear()

        b.created = timestamp
        b.numb = 2
        b.title = "title1"
        b.insert()
        b.clear()

        b.created = timestamp
        b.numb = 5
        b.title = "title2"
        b.insert()
        b.clear()

        lookup = FieldsLookup(a, b).add("date", "created")
        a.setIn(lookup)
        self.assertEqual(2, a.count())

        lookup = FieldsLookup(a, b).add("date", "created").add("number", "numb")
        a.setIn(lookup)
        self.assertEqual(1, a.count())

        lookup = FieldsLookup(a, b).add("date", "created").add("number", "numb").add("description", "title")
        a.setIn(lookup)
        self.assertEqual(0, a.count())


    def testExceptionWhileAddingNotExistedFieldsToLookup(self):
        a = aFilterCursor(self.context)
        b = bFilterCursor(self.context)

        lookup = FieldsLookup(a, b)

        with self.assertRaises(ParseException) as context:
            lookup.add("notExistingField", "created")

        with self.assertRaises(ParseException) as context:
            lookup.add("date", "notExistingField")


        with self.assertRaises(ParseException) as context:
            lookup.add("notExistingField", "notExistingField")



    def testExceptionWhileAddingFieldsWithNotMatchesTypesToLookup(self):
        a = aFilterCursor(self.context)
        b = bFilterCursor(self.context)

        lookup = FieldsLookup(a, b)

        with self.assertRaises(CelestaException) as context:
            lookup.add("date", "numb")

        self.assertTrue(isinstance(context.exception, CelestaException))


    def testExceptionWhileAddingFieldsWithoutIndexToLookup(self):
        a = aFilterCursor(self.context)
        b = bFilterCursor(self.context)

        lookup = FieldsLookup(a, b)

        with self.assertRaises(CelestaException) as context:
            lookup.add("noIndexA", "numb")

        self.assertTrue(isinstance(context.exception, CelestaException))

        with self.assertRaises(CelestaException) as context:
            lookup.add("number", "noIndexB")

        self.assertTrue(isinstance(context.exception, CelestaException))

        with self.assertRaises(CelestaException) as context:
            lookup.add("noIndexA", "noIndexB")

            self.assertTrue(isinstance(context.exception, CelestaException))


    def testExceptionWhenIndexNotExists(self):
        a = aFilterCursor(self.context)
        b = bFilterCursor(self.context)

        lookup = FieldsLookup(a, b).add("a1", "numb").add("a2", "numb")

        with self.assertRaises(CelestaException) as context:
            a.setIn(lookup)

        self.assertTrue(isinstance(context.exception, CelestaException))

        lookup = FieldsLookup(a, b).add("number", "b2").add("number", "b2")

        with self.assertRaises(CelestaException) as context:
            a.setIn(lookup)

        self.assertTrue(isinstance(context.exception, CelestaException))

        lookup = FieldsLookup(a, b).add("a1", "b2").add("a2", "b2")

        with self.assertRaises(CelestaException) as context:
            a.setIn(lookup)

        self.assertTrue(isinstance(context.exception, CelestaException))

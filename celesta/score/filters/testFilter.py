from celestaunit.internal_celesta_unit import CelestaUnit

from java.time import LocalDateTime
from java.sql import Timestamp
from ru.curs.celesta.dbutils.filter.value import FieldsLookup

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

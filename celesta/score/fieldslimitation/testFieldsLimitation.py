# coding=UTF-8

from celestaunit.internal_celesta_unit import CelestaUnit
from fieldslimitation._fieldslimitation_orm import aCursor, avCursor, amvCursor

class TestFieldsLimitation(CelestaUnit):

    def test_get_on_table(self):
        tableCursor = aCursor(self.context, ['numb', 'var'])
        tableCursor.deleteAll()

        tableCursor.numb = 5
        tableCursor.var = "A"
        tableCursor.insert()
        id1 = tableCursor.id
        tableCursor.clear()

        tableCursor.numb = 2
        tableCursor.var = "B"
        tableCursor.insert()
        id2 = tableCursor.id
        tableCursor.clear()

        tableCursor.get(id1)
        self.assertEqual(None, tableCursor.id)
        self.assertEqual(5, tableCursor.numb)
        self.assertEqual("A", tableCursor.var)
        self.assertEqual(None, tableCursor.age)

        tableCursor.tryGet(id1)
        self.assertEqual(None, tableCursor.id)
        self.assertEqual(5, tableCursor.numb)
        self.assertEqual("A", tableCursor.var)
        self.assertEqual(None, tableCursor.age)

        tableCursor.get(id2)
        self.assertEqual(None, tableCursor.id)
        self.assertEqual(2, tableCursor.numb)
        self.assertEqual("B", tableCursor.var)
        self.assertEqual(None, tableCursor.age)

        tableCursor.tryGet(id2)
        self.assertEqual(None, tableCursor.id)
        self.assertEqual(2, tableCursor.numb)
        self.assertEqual("B", tableCursor.var)
        self.assertEqual(None, tableCursor.age)

    def test_get_on_materialized_view(self):
        tableCursor = aCursor(self.context)
        mvCursor = amvCursor(self.context, ['s'])
        tableCursor.deleteAll()

        tableCursor.numb = 5
        tableCursor.var = "A"
        tableCursor.insert()
        tableCursor.clear()

        tableCursor.numb = 2
        tableCursor.var = "B"
        tableCursor.insert()
        tableCursor.clear()

        mvCursor.get("A")
        self.assertEqual(5, mvCursor.s)
        self.assertEqual(None, mvCursor.var)

        mvCursor.tryGet("A")
        self.assertEqual(5, mvCursor.s)
        self.assertEqual(None, mvCursor.var)

        mvCursor.get("B")
        self.assertEqual(2, mvCursor.s)
        self.assertEqual(None, mvCursor.var)

        mvCursor.tryGet("B")
        self.assertEqual(2, mvCursor.s)
        self.assertEqual(None, mvCursor.var)

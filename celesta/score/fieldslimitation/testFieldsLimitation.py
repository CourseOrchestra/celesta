# coding=UTF-8

from celestaunit.internal_celesta_unit import CelestaUnit
from fieldslimitation._fieldslimitation_orm import aCursor, avCursor, amvCursor

class TestFieldsLimitation(CelestaUnit):

    def test_get_on_table(self):
        self._clear_table()
        id1 = self._insert("A", 5, 1)
        id2 = self._insert("B", 2, 4)
        tableCursor = aCursor(self.context, ['numb', 'var'])

        tableCursor.get(id1)
        self.assertEqual(id1, tableCursor.id)
        self.assertEqual(5, tableCursor.numb)
        self.assertEqual("A", tableCursor.var)
        self.assertEqual(None, tableCursor.age)

        tableCursor.get(id2)
        self.assertEqual(id2, tableCursor.id)
        self.assertEqual(2, tableCursor.numb)
        self.assertEqual("B", tableCursor.var)
        self.assertEqual(None, tableCursor.age)

    def test_get_on_materialized_view(self):
        self._clear_table()
        self._insert("A", 5, 1)
        self._insert("B", 2, 4)

        mvCursor = amvCursor(self.context, ['numb'])

        mvCursor.get("A")
        self.assertEqual(None, mvCursor.id)
        self.assertEqual(5, mvCursor.numb)
        self.assertEqual("A", mvCursor.var)
        self.assertEqual(None, mvCursor.age)

        mvCursor.get("B")
        self.assertEqual(None, mvCursor.id)
        self.assertEqual(2, mvCursor.numb)
        self.assertEqual("B", mvCursor.var)
        self.assertEqual(None, mvCursor.age)

    def test_set_on_table(self):
        tableCursor = aCursor(self.context, ['numb', 'var'])
        self._test_set(tableCursor)

    def test_set_on_view(self):
        viewCursor = avCursor(self.context, ['numb', 'var'])
        self._test_set(viewCursor)

    def test_set_on_materialized_view(self):
        viewCursor = avCursor(self.context, ['numb', 'var'])
        self._test_set(viewCursor)

    def test_navigation_on_table(self):
        tableCursor = aCursor(self.context, ['numb', 'var'])
        self._test_navigation(tableCursor)

    def test_navigation_on_view(self):
        viewCursor = avCursor(self.context, ['numb', 'var'])
        self._test_navigation(viewCursor)

    def test_navigation_on_materialized_view(self):
        viewCursor = avCursor(self.context, ['numb', 'var'])
        self._test_navigation(viewCursor)

    def _test_set(self, cursor):
        self._clear_table()
        self._insert("A", 5, 1)
        self._insert("B", 2, 4)

        cursor.orderBy("numb DESC")
        cursor.findSet()
        self.assertEqual(5, cursor.numb)
        self.assertEqual("A", cursor.var)
        self.assertEqual(None, cursor.age)

        cursor.nextInSet()
        self.assertEqual(2, cursor.numb)
        self.assertEqual("B", cursor.var)
        self.assertEqual(None, cursor.age)

    def _test_navigation(self, cursor):
        self._clear_table()
        self._insert("A", 5, 1)
        self._insert("B", 2, 4)

        cursor.orderBy("numb DESC")
        cursor.first()
        self.assertEqual(5, cursor.numb)
        self.assertEqual("A", cursor.var)
        self.assertEqual(None, cursor.age)

        cursor.next()
        self.assertEqual(2, cursor.numb)
        self.assertEqual("B", cursor.var)
        self.assertEqual(None, cursor.age)

        cursor.navigate("=")
        self.assertEqual(2, cursor.numb)
        self.assertEqual("B", cursor.var)
        self.assertEqual(None, cursor.age)

        cursor.previous()
        self.assertEqual(5, cursor.numb)
        self.assertEqual("A", cursor.var)
        self.assertEqual(None, cursor.age)

        cursor.last()
        self.assertEqual(2, cursor.numb)
        self.assertEqual("B", cursor.var)
        self.assertEqual(None, cursor.age)

    def _clear_table(self):
        tableCursor = aCursor(self.context)
        tableCursor.deleteAll()

    def _insert(self, var, numb, age):
        tableCursor = aCursor(self.context)

        tableCursor.var = var
        tableCursor.numb = numb
        tableCursor.age = age
        tableCursor.insert()
        id = tableCursor.id
        tableCursor.clear()
        return id

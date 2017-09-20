# coding=UTF-8

from fieldslimitation._fieldslimitation_orm import aCursor, avCursor, amvCursor
from ru.curs.celesta.unit import TestClass, CelestaTestCase

@TestClass
class TestFieldsLimitation(CelestaTestCase):

    def test_get_on_table(self):
        self._clear_table()
        id1 = self._insert("A", 5, 1)
        id2 = self._insert("B", 2, 4)
        tableCursor = aCursor(self.context, ['numb', 'var'])

        tableCursor.get(id1)
        self.assertEquals(id1, tableCursor.id)
        self.assertEquals(5, tableCursor.numb)
        self.assertEquals("A", tableCursor.var)
        self.assertEquals(None, tableCursor.age)

        tableCursor.get(id2)
        self.assertEquals(id2, tableCursor.id)
        self.assertEquals(2, tableCursor.numb)
        self.assertEquals("B", tableCursor.var)
        self.assertEquals(None, tableCursor.age)

    def test_get_on_materialized_view(self):
        self._clear_table()
        self._insert("A", 5, 1)
        self._insert("B", 2, 4)

        mvCursor = amvCursor(self.context, ['numb'])

        mvCursor.get("A")
        self.assertEquals(None, mvCursor.id)
        self.assertEquals(5, mvCursor.numb)
        self.assertEquals("A", mvCursor.var)
        self.assertEquals(None, mvCursor.age)

        mvCursor.get("B")
        self.assertEquals(None, mvCursor.id)
        self.assertEquals(2, mvCursor.numb)
        self.assertEquals("B", mvCursor.var)
        self.assertEquals(None, mvCursor.age)

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
        self.assertEquals(5, cursor.numb)
        self.assertEquals("A", cursor.var)
        self.assertEquals(None, cursor.age)

        cursor.nextInSet()
        self.assertEquals(2, cursor.numb)
        self.assertEquals("B", cursor.var)
        self.assertEquals(None, cursor.age)

    def _test_navigation(self, cursor):
        self._clear_table()
        self._insert("A", 5, 1)
        self._insert("B", 2, 4)

        cursor.orderBy("numb DESC")
        cursor.first()
        self.assertEquals(5, cursor.numb)
        self.assertEquals("A", cursor.var)
        self.assertEquals(None, cursor.age)

        cursor.next()
        self.assertEquals(2, cursor.numb)
        self.assertEquals("B", cursor.var)
        self.assertEquals(None, cursor.age)

        cursor.navigate("=")
        self.assertEquals(2, cursor.numb)
        self.assertEquals("B", cursor.var)
        self.assertEquals(None, cursor.age)

        cursor.previous()
        self.assertEquals(5, cursor.numb)
        self.assertEquals("A", cursor.var)
        self.assertEquals(None, cursor.age)

        cursor.last()
        self.assertEquals(2, cursor.numb)
        self.assertEquals("B", cursor.var)
        self.assertEquals(None, cursor.age)

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

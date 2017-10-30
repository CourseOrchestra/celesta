# coding=UTF-8

from navigation._navigation_orm import navigationTableCursor
from ru.curs.celesta.unit import TestClass, CelestaTestCase

@TestClass
class TestNavigation(CelestaTestCase):

    def testSimpleNext(self):
        c = navigationTableCursor(self.context)
        self._prepareTableForTest(c)

        c.orderBy('numb')
        c.first()
        self.assertEquals(1, c.numb)
        c.next()
        self.assertEquals(2, c.numb)
        c.next()
        self.assertEquals(3, c.numb)
        c.next()
        self.assertEquals(4, c.numb)
        c.next()
        self.assertEquals(5, c.numb)
        c.next()
        self.assertEquals(5, c.numb)


    def testSimplePrevious(self):
        c = navigationTableCursor(self.context)
        self._prepareTableForTest(c)

        c.orderBy('numb')
        c.last()
        self.assertEquals(5, c.numb)
        c.previous()
        self.assertEquals(4, c.numb)
        c.previous()
        self.assertEquals(3, c.numb)
        c.previous()
        self.assertEquals(2, c.numb)
        c.previous()
        self.assertEquals(1, c.numb)
        c.previous()
        self.assertEquals(1, c.numb)


    def testNavigateWithOffset(self):
        c = navigationTableCursor(self.context)
        self._prepareTableForTest(c)

        c.orderBy('numb')
        c.first()
        self.assertEquals(1, c.numb)

        c.navigate('>', 3)
        self.assertEquals(4, c.numb)

        c.navigate('<', 2)
        self.assertEquals(2, c.numb)

        self.assertFalse(c.navigate('<', 10))
        self.assertEquals(2, c.numb)

        self.assertFalse(c.navigate('>', 10))
        self.assertEquals(2, c.numb)


    def _prepareTableForTest(self, c):
        c.deleteAll()
        self._insert(c, 1)
        self._insert(c, 2)
        self._insert(c, 3)
        self._insert(c, 4)
        self._insert(c, 5)
        c.clear()


    def _insert(self, c, numb):
        c.clear()
        c.numb = numb
        c.insert()
        c.clear()
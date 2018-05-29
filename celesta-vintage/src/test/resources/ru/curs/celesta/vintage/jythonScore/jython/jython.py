# coding=UTF-8

from _jython_orm import jythonTableCursor
from org.junit.jupiter.api import Assertions

def testJythonTable(context):
    c = jythonTableCursor(context)
    Assertions.assertEquals(0, c.count())

    c.val = 1
    c.insert()
    c.clear()
    Assertions.assertEquals(1, c.count())

    c.first()
    Assertions.assertEquals(1, c.id)
    Assertions.assertEquals(1, c.val)

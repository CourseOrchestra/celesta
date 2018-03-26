# coding=UTF-8

from _testTable_orm import tBlobCursor, tXRecCursor, tCsvLineCursor, tIterateCursor, \
    tCopyFieldsCursor, tLimitCursor, tWithDecimalCursor

from ru.curs.celesta import CelestaException
from ru.curs.celesta.unit import TestClass, CelestaTestCase

from java.sql import Timestamp
from java.time import LocalDateTime, Month
from java.math import BigDecimal

import java.io.OutputStreamWriter as OutputStreamWriter
import java.io.InputStreamReader as InputStreamReader
import java.io.BufferedReader as BufferedReader

@TestClass
class TestTable(CelestaTestCase):

    def test_calc_blob(self):
        cursor = tBlobCursor(self.context)
        cursor.deleteAll()

        cursor.insert()
        cursor.get(1)

        self.assertEquals(1, cursor.id)
        self.assertEquals(None, cursor.dat)

        cursor.calcdat()

        self.assertTrue(cursor.dat and cursor.dat.isNull())

        os = cursor.dat.getOutStream()
        osw = OutputStreamWriter(os, 'utf-8')
        try:
            osw.append('blob field')
        finally:
            osw.close()

        cursor.update()
        cursor.clear()
        cursor.get(1)
        cursor.calcdat()
        bf = BufferedReader(InputStreamReader(cursor.dat.getInStream(), 'utf-8'))
        self.assertEquals('blob field', bf.readLine())
        bf.close()

        cursor.clear()
        cursor.calcdat()
        os = cursor.dat.getOutStream()
        osw = OutputStreamWriter(os, 'utf-8')
        try:
            osw.append('blob field 2!')
        finally:
            osw.close()
        cursor.insert()

        cursor.clear()
        cursor.get(2)
        cursor.calcdat()
        bf = BufferedReader(InputStreamReader(cursor.dat.getInStream(), 'utf-8'))
        self.assertEquals('blob field 2!', bf.readLine())
        bf.close()

    def test_getXRec(self):
        cursor = tXRecCursor(self.context)
        cursor.deleteAll()

        id = 1
        num = 10
        cost = 10.2
        title = 'product'
        isActive = True
        created = Timestamp.valueOf(LocalDateTime.of(2018, Month.of(1), 11, 19, 15))

        cursor.num = num
        cursor.cost = cost
        cursor.title = title
        cursor.isActive = isActive
        cursor.created = created

        xRec = cursor.getXRec()
        self._assertXRecCursorFields(xRec, None, None, None, None, None, None)
        cursor.insert()

        self._assertXRecCursorFields(xRec, None, None, None, None, None, None)
        cursor.clear()

        xRec = cursor.getXRec()
        self._assertXRecCursorFields(xRec, None, None, None, None, None, None)

        cursor.get(1)
        self._assertXRecCursorFields(xRec, id, num, cost, title, isActive, created)

        cursor.num = num + 1
        cursor.cost = cost + 1
        cursor.title = title + 'asd'
        cursor.isActive = False
        cursor.created = Timestamp.valueOf(LocalDateTime.of(2017, Month.of(1), 11, 19, 15))

        self._assertXRecCursorFields(xRec, id, num, cost, title, isActive, created)

    def test_asCSVLine(self):
        cursor = tCsvLineCursor(self.context)
        self.assertEquals('NULL,NULL',cursor.asCSVLine())

        cursor.id = 1
        self.assertEquals('1,NULL',cursor.asCSVLine())

        cursor.title = 'noQuotes'
        self.assertEquals('1,noQuotes',cursor.asCSVLine())

        cursor.title = '"withQuotes"'
        self.assertEquals('1,"""withQuotes"""',cursor.asCSVLine())

        cursor.title = None
        self.assertEquals('1,NULL',cursor.asCSVLine())

    def test_iterate(self):
        cursor = tIterateCursor(self.context)
        cursor.insert()
        cursor.insert()

        idList = []

        for cursor in cursor.iterate():
            idList.append(cursor.id)

        self.assertEquals(2, idList.__len__())
        self.assertEquals(1, idList[0])
        self.assertEquals(2, idList[1])

    def test_CopyFieldsFrom(self):
        cursor = tCopyFieldsCursor(self.context)
        cursorFrom = tCopyFieldsCursor(self.context)

        id = 11234
        title = 'ttt'

        cursorFrom.id = id
        cursorFrom.title = title
        cursor.copyFieldsFrom(cursorFrom)

        self.assertEquals(id, cursor.id)
        self.assertEquals(title, cursor.title)

    def test_limit(self):
        cursor = tLimitCursor(self.context)

        for i in range(3):
            cursor.insert()

        idList = []

        cursor.limit(0, 2)
        for cursor in cursor.iterate():
            idList.append(cursor.id)
        self.assertEquals(2, idList.__len__())
        self.assertEquals(1, idList[0])
        self.assertEquals(2, idList[1])
        del idList[:]

        cursor.limit(2, 1)
        for cursor in cursor.iterate():
            idList.append(cursor.id)
        self.assertEquals(1, idList.__len__())
        self.assertEquals(3, idList[0])
        del idList[:]

        cursor.limit(3, 0)
        for cursor in cursor.iterate():
            idList.append(cursor.id)
        self.assertEquals(0, idList.__len__())
        del idList[:]

        cursor.limit(3, 5)
        for cursor in cursor.iterate():
            idList.append(cursor.id)
        self.assertEquals(0, idList.__len__())

    def test_decimal(self):
        c = tWithDecimalCursor(self.context)

        c.insert()
        c.first()
        self.assertEquals(BigDecimal('5.2'), c.cost.stripTrailingZeros())

        c.cost = BigDecimal('5.289')
        c.update()
        c.first()
        self.assertEquals(BigDecimal('5.29'), c.cost.stripTrailingZeros())

        c.cost = BigDecimal('123.2')
        update = lambda : c.update()
        self.assertThrows(CelestaException, update)


    def _assertXRecCursorFields(self, cursor, id, num, cost, title, isActive, created):
        self.assertEquals(id, cursor.id)
        self.assertEquals(num, cursor.num)
        self.assertEquals(cost, cursor.cost)
        self.assertEquals(title, cursor.title)
        self.assertEquals(isActive, cursor.isActive)
        self.assertEquals(created, cursor.created)
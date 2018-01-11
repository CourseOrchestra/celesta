# coding=UTF-8

from _testTable_orm import tBlobCursor, tXRecCursor

from ru.curs.celesta.unit import TestClass, CelestaTestCase

from java.sql import Timestamp
from java.time import LocalDateTime, Month

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


    def _assertXRecCursorFields(self, cursor, id, num, cost, title, isActive, created):
        self.assertEquals(id, cursor.id)
        self.assertEquals(num, cursor.num)
        self.assertEquals(cost, cursor.cost)
        self.assertEquals(title, cursor.title)
        self.assertEquals(isActive, cursor.isActive)
        self.assertEquals(created, cursor.created)
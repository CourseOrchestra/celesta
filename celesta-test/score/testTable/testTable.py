# coding=UTF-8

from _testTable_orm import tBlobCursor

from ru.curs.celesta.unit import TestClass, CelestaTestCase

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







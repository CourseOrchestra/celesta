# coding=UTF-8

from ru.curs.celesta.unit import TestClass, CelestaTestCase
from ru.curs.celesta import CelestaException

from sequences._sequences_orm import s1Cursor, s2Cursor, s3Cursor, s4Cursor, s5Cursor, \
    s6Cursor, s7Cursor, s8Cursor, s9Cursor, s10Cursor, \
    s11Cursor, s12Cursor, s13Cursor, s14Cursor, s15Cursor

@TestClass
class TestSequence(CelestaTestCase):

    def testS1(self):
        s = s1Cursor(self.context)

        self.assertEquals(1L, s.nextValue())
        self.assertEquals(2L, s.nextValue())

    def testS2(self):
        s = s2Cursor(self.context)

        self.assertEquals(3L, s.nextValue())
        self.assertEquals(4L, s.nextValue())

    def testS3(self):
        s = s3Cursor(self.context)

        self.assertEquals(1L, s.nextValue())
        self.assertEquals(3L, s.nextValue())

    def testS4(self):
        s = s4Cursor(self.context)

        self.assertEquals(1L, s.nextValue())
        self.assertEquals(-1L, s.nextValue())

    def testS5(self):
        s = s5Cursor(self.context)

        self.assertEquals(5L, s.nextValue())
        self.assertEquals(4L, s.nextValue())

    def testS6(self):
        s = s6Cursor(self.context)

        self.assertEquals(5L, s.nextValue())
        self.assertEquals(4L, s.nextValue())
        #end of sequence
        self.assertThrows(CelestaException, nextVal, s)

    def testS7(self):
        s = s7Cursor(self.context)

        self.assertEquals(-3L, s.nextValue())
        self.assertEquals(-5L, s.nextValue())
        #end of sequence
        self.assertThrows(CelestaException, nextVal, s)

    def testS8(self):
        s = s8Cursor(self.context)

        self.assertEquals(-1L, s.nextValue())
        #end of sequence
        self.assertThrows(CelestaException, nextVal, s)

    def testS9(self):
        s = s9Cursor(self.context)

        self.assertEquals(5L, s.nextValue())
        self.assertEquals(6L, s.nextValue())

    def testS10(self):
        s = s10Cursor(self.context)

        self.assertEquals(-5L, s.nextValue())
        self.assertEquals(-4L, s.nextValue())

    def testS11(self):
        s = s11Cursor(self.context)

        self.assertEquals(1L, s.nextValue())
        self.assertEquals(2L, s.nextValue())

    def testS12(self):
        s = s12Cursor(self.context)

        self.assertEquals(1L, s.nextValue())
        self.assertEquals(2L, s.nextValue())
        self.assertEquals(3L, s.nextValue())
        self.assertEquals(4L, s.nextValue())
        self.assertEquals(5L, s.nextValue())
        #new cycle
        self.assertEquals(1L, s.nextValue())

    def testS13(self):
        s = s13Cursor(self.context)

        self.assertEquals(1L, s.nextValue())
        self.assertEquals(0L, s.nextValue())
        self.assertEquals(-1L, s.nextValue())
        #new cycle
        self.assertEquals(1L, s.nextValue())

    def testS14AndS15(self):
        self._testS14OrS15(s14Cursor(self.context))
        self._testS14OrS15(s15Cursor(self.context))

    def _testS14OrS15(self, s):
        lastValue = s.nextValue()
        self.assertEquals(5L, lastValue)
        lastValue = s.nextValue()
        self.assertEquals(7L, lastValue)

        while (lastValue < 55):
            lastValue = s.nextValue()

        #new cycle
        lastValue = s.nextValue()
        self.assertEquals(5L, lastValue)

def nextVal(s):
    s.nextValue()
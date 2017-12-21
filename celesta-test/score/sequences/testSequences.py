# coding=UTF-8

from ru.curs.celesta.unit import TestClass, CelestaTestCase
from ru.curs.celesta import CelestaException

from sequences._sequences_orm import s1, s2, s3, s4, s5, \
    s6, s7, s8, s9, s10, \
    s11, s12, s13, s14, s15, t1Cursor

@TestClass
class TestSequence(CelestaTestCase):

    def testS1(self):
        s = s1(self.context)

        self.assertEquals(1L, s.nextValue())
        self.assertEquals(2L, s.nextValue())

    def testS2(self):
        s = s2(self.context)

        self.assertEquals(3L, s.nextValue())
        self.assertEquals(4L, s.nextValue())

    def testS3(self):
        s = s3(self.context)

        self.assertEquals(1L, s.nextValue())
        self.assertEquals(3L, s.nextValue())

    def testS4(self):
        s = s4(self.context)

        self.assertEquals(1L, s.nextValue())
        self.assertEquals(-1L, s.nextValue())

    def testS5(self):
        s = s5(self.context)

        self.assertEquals(5L, s.nextValue())
        self.assertEquals(4L, s.nextValue())

    def testS6(self):
        s = s6(self.context)

        self.assertEquals(5L, s.nextValue())
        self.assertEquals(4L, s.nextValue())
        #end of sequence
        self.assertThrows(CelestaException, nextVal, s)

    def testS7(self):
        s = s7(self.context)

        self.assertEquals(-3L, s.nextValue())
        self.assertEquals(-5L, s.nextValue())
        #end of sequence
        self.assertThrows(CelestaException, nextVal, s)

    def testS8(self):
        s = s8(self.context)

        self.assertEquals(-1L, s.nextValue())
        #end of sequence
        self.assertThrows(CelestaException, nextVal, s)

    def testS9(self):
        s = s9(self.context)

        self.assertEquals(5L, s.nextValue())
        self.assertEquals(6L, s.nextValue())

    def testS10(self):
        s = s10(self.context)

        self.assertEquals(-5L, s.nextValue())
        self.assertEquals(-4L, s.nextValue())

    def testS11(self):
        s = s11(self.context)

        self.assertEquals(1L, s.nextValue())
        self.assertEquals(2L, s.nextValue())

    def testS12(self):
        s = s12(self.context)

        self.assertEquals(1L, s.nextValue())
        self.assertEquals(2L, s.nextValue())
        self.assertEquals(3L, s.nextValue())
        self.assertEquals(4L, s.nextValue())
        self.assertEquals(5L, s.nextValue())
        #new cycle
        self.assertEquals(1L, s.nextValue())

    def testS13(self):
        s = s13(self.context)

        self.assertEquals(1L, s.nextValue())
        self.assertEquals(0L, s.nextValue())
        self.assertEquals(-1L, s.nextValue())
        #new cycle
        self.assertEquals(1L, s.nextValue())

    def testS14AndS15(self):
        self._testS14OrS15(s14(self.context))
        self._testS14OrS15(s15(self.context))

    def testDefaultPkColumnValueWithSequence(self):
        c = t1Cursor(self.context)

        c.insert()
        c.last()
        self.assertEquals(4, c.id)
        c.clear()
        c.insert()
        c.last()
        self.assertEquals(6, c.id)


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
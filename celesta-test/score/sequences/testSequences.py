# coding=UTF-8

from ru.curs.celesta.unit import TestClass, CelestaTestCase
from ru.curs.celesta import CelestaException

from sequences._sequences_orm import s1Sequence, s2Sequence, s3Sequence, s4Sequence, s5Sequence, \
    s6Sequence, s7Sequence, s8Sequence, s9Sequence, s10Sequence, \
    s11Sequence, s12Sequence, s13Sequence, s14Sequence, s15Sequence, t1Cursor

@TestClass
class TestSequence(CelestaTestCase):

    def testS1(self):
        s = s1Sequence(self.context)

        self.assertEquals(1L, s.nextValue())
        self.assertEquals(2L, s.nextValue())

    def testS2(self):
        s = s2Sequence(self.context)

        self.assertEquals(3L, s.nextValue())
        self.assertEquals(4L, s.nextValue())

    def testS3(self):
        s = s3Sequence(self.context)

        self.assertEquals(1L, s.nextValue())
        self.assertEquals(3L, s.nextValue())

    def testS4(self):
        s = s4Sequence(self.context)

        self.assertEquals(1L, s.nextValue())
        self.assertEquals(-1L, s.nextValue())

    def testS5(self):
        s = s5Sequence(self.context)

        self.assertEquals(5L, s.nextValue())
        self.assertEquals(4L, s.nextValue())

    def testS6(self):
        s = s6Sequence(self.context)

        self.assertEquals(5L, s.nextValue())
        self.assertEquals(4L, s.nextValue())
        #end of sequence
        self.assertThrows(CelestaException, nextVal, s)

    def testS7(self):
        s = s7Sequence(self.context)

        self.assertEquals(-3L, s.nextValue())
        self.assertEquals(-5L, s.nextValue())
        #end of sequence
        self.assertThrows(CelestaException, nextVal, s)

    def testS8(self):
        s = s8Sequence(self.context)

        self.assertEquals(-1L, s.nextValue())
        #end of sequence
        self.assertThrows(CelestaException, nextVal, s)

    def testS9(self):
        s = s9Sequence(self.context)

        self.assertEquals(5L, s.nextValue())
        self.assertEquals(6L, s.nextValue())

    def testS10(self):
        s = s10Sequence(self.context)

        self.assertEquals(-5L, s.nextValue())
        self.assertEquals(-4L, s.nextValue())

    def testS11(self):
        s = s11Sequence(self.context)

        self.assertEquals(1L, s.nextValue())
        self.assertEquals(2L, s.nextValue())

    def testS12(self):
        s = s12Sequence(self.context)

        self.assertEquals(1L, s.nextValue())
        self.assertEquals(2L, s.nextValue())
        self.assertEquals(3L, s.nextValue())
        self.assertEquals(4L, s.nextValue())
        self.assertEquals(5L, s.nextValue())
        #new cycle
        self.assertEquals(1L, s.nextValue())

    def testS13(self):
        s = s13Sequence(self.context)

        self.assertEquals(1L, s.nextValue())
        self.assertEquals(0L, s.nextValue())
        self.assertEquals(-1L, s.nextValue())
        #new cycle
        self.assertEquals(1L, s.nextValue())

    def testS14AndS15(self):
        self._testS14OrS15(s14Sequence(self.context))
        self._testS14OrS15(s15Sequence(self.context))

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
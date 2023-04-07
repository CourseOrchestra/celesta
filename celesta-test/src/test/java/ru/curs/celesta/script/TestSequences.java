package ru.curs.celesta.script;

import org.junit.jupiter.api.TestTemplate;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.Sequence;
import sequences.S10Sequence;
import sequences.S11Sequence;
import sequences.S12Sequence;
import sequences.S13Sequence;
import sequences.S14Sequence;
import sequences.S15Sequence;
import sequences.S1Sequence;
import sequences.S2Sequence;
import sequences.S3Sequence;
import sequences.S4Sequence;
import sequences.S5Sequence;
import sequences.S6Sequence;
import sequences.S7Sequence;
import sequences.S8Sequence;
import sequences.S9Sequence;
import sequences.T1Cursor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


class TestSequences implements ScriptTest {

    @TestTemplate
    void testS1(CallContext context) {
        S1Sequence s = new S1Sequence(context);

        assertEquals(1L, s.nextValue());
        assertEquals(2L, s.nextValue());
        assertEquals(3L, s.nextValue());
    }

    @TestTemplate
    void testS2(CallContext context) {
        S2Sequence s = new S2Sequence(context);

        assertEquals(3L, s.nextValue());
        assertEquals(4L, s.nextValue());
        assertEquals(5L, s.nextValue());
    }

    @TestTemplate
    void testS3(CallContext context) {
        S3Sequence s = new S3Sequence(context);

        assertEquals(1L, s.nextValue());
        assertEquals(3L, s.nextValue());
        assertEquals(5L, s.nextValue());
    }

    @TestTemplate
    void testS4(CallContext context) {
        S4Sequence s = new S4Sequence(context);

        assertEquals(1L, s.nextValue());
        assertEquals(-1L, s.nextValue());
        assertEquals(-3L, s.nextValue());
    }

    @TestTemplate
    void testS5(CallContext context) {
        S5Sequence s = new S5Sequence(context);

        assertEquals(5L, s.nextValue());
        assertEquals(4L, s.nextValue());
    }

    @TestTemplate
    void testS6(CallContext context) {
        S6Sequence s = new S6Sequence(context);

        assertEquals(5L, s.nextValue());
        assertEquals(4L, s.nextValue());

        //end of sequence
        assertThrows(CelestaException.class,
                s::nextValue);
    }

    @TestTemplate
    void testS7(CallContext context) {
        S7Sequence s = new S7Sequence(context);

        assertEquals(-3L, s.nextValue());
        assertEquals(-5L, s.nextValue());
        //end of sequence
        assertThrows(CelestaException.class,
                s::nextValue);
    }

    @TestTemplate
    void testS8(CallContext context) {
        S8Sequence s = new S8Sequence(context);
        assertEquals(-1L, s.nextValue());
        //end of sequence
        assertThrows(CelestaException.class,
                s::nextValue);

    }

    @TestTemplate
    void testS9(CallContext context) {
        S9Sequence s = new S9Sequence(context);

        assertEquals(5L, s.nextValue());
        assertEquals(6L, s.nextValue());

    }

    @TestTemplate
    void testS10(CallContext context) {
        S10Sequence s = new S10Sequence(context);

        assertEquals(-5L, s.nextValue());
        assertEquals(-4L, s.nextValue());

    }

    @TestTemplate
    void testS11(CallContext context) {
        S11Sequence s = new S11Sequence(context);

        assertEquals(1L, s.nextValue());
        assertEquals(2L, s.nextValue());

    }

    @TestTemplate
    void testS12(CallContext context) {
        S12Sequence s = new S12Sequence(context);

        assertEquals(1L, s.nextValue());
        assertEquals(2L, s.nextValue());
        assertEquals(3L, s.nextValue());
        assertEquals(4L, s.nextValue());
        assertEquals(5L, s.nextValue());
        //new cycle;
        assertEquals(1L, s.nextValue());

    }

    @TestTemplate
    void testS13(CallContext context) {
        S13Sequence s = new S13Sequence(context);

        assertEquals(1L, s.nextValue());
        assertEquals(0L, s.nextValue());
        assertEquals(-1L, s.nextValue());
        //new cycle;
        assertEquals(1L, s.nextValue());

    }

    @TestTemplate
    void testS14AndS15(CallContext context) {
        _testS14OrS15(new S14Sequence(context));
        _testS14OrS15(new S15Sequence(context));

    }

    @TestTemplate
    void testDefaultPkColumnValueWithSequence(CallContext context) {
        T1Cursor c = new T1Cursor(context);

        c.insert();
        assertEquals(4, c.getId().intValue());
        c.clear();

        c.insert();
        assertEquals(6, c.getId().intValue());


    }

    void _testS14OrS15(Sequence s) {
        long lastValue = s.nextValue();
        assertEquals(5L, lastValue);
        lastValue = s.nextValue();
        assertEquals(7L, lastValue);

        while (lastValue < 55) {
            lastValue = s.nextValue();
        }
        //new cycle;
        lastValue = s.nextValue();
        assertEquals(5L, lastValue);
    }
}

package ru.curs.celesta.score;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static ru.curs.celesta.score.Sequence.Argument.*;

public class SequenceParsingTest extends AbstractParsingTest {

    @Test
    void testParsingOnCorrectSyntax() throws ParseException {
        ChecksumInputStream input = new ChecksumInputStream(
                ParserTest.class.getResourceAsStream(
                        "sequence/testParsingOnCorrectSyntax.sql"
                ));
        CelestaParser cp = new CelestaParser(input);
        Grain g = cp.grain(s, "test");

        Sequence s1 = g.getElement("s1", Sequence.class);
        assertAll(
                () -> assertEquals(1, s1.getArgument(START_WITH)),
                () -> assertEquals(1, s1.getArgument(INCREMENT_BY)),
                () -> assertFalse(s1.hasArgument(MINVALUE)),
                () -> assertFalse(s1.hasArgument(MAXVALUE)),
                () -> assertFalse(s1.hasArgument(CYCLE))
        );

        Sequence s2 = g.getElement("s2", Sequence.class);
        assertAll(
                () -> assertEquals(3, s2.getArgument(START_WITH)),
                () -> assertEquals(1, s2.getArgument(INCREMENT_BY)),
                () -> assertFalse(s2.hasArgument(MINVALUE)),
                () -> assertFalse(s2.hasArgument(MAXVALUE)),
                () -> assertFalse(s2.hasArgument(CYCLE))
        );

        Sequence s3 = g.getElement("s3", Sequence.class);
        assertAll(
                () -> assertEquals(1, s3.getArgument(START_WITH)),
                () -> assertEquals(2, s3.getArgument(INCREMENT_BY)),
                () -> assertFalse(s3.hasArgument(MINVALUE)),
                () -> assertFalse(s3.hasArgument(MAXVALUE)),
                () -> assertFalse(s3.hasArgument(CYCLE))
        );

        Sequence s4 = g.getElement("s4", Sequence.class);
        assertAll(
                () -> assertEquals(1, s4.getArgument(START_WITH)),
                () -> assertEquals(-2, s4.getArgument(INCREMENT_BY)),
                () -> assertFalse(s4.hasArgument(MINVALUE)),
                () -> assertEquals(1, s4.getArgument(MAXVALUE)),
                () -> assertFalse(s4.hasArgument(CYCLE))
        );

        Sequence s5 = g.getElement("s5", Sequence.class);
        assertAll(
                () -> assertEquals(5, s5.getArgument(START_WITH)),
                () -> assertEquals(-1, s5.getArgument(INCREMENT_BY)),
                () -> assertFalse(s5.hasArgument(MINVALUE)),
                () -> assertEquals(5, s5.getArgument(MAXVALUE)),
                () -> assertFalse(s5.hasArgument(CYCLE))
        );

        Sequence s6 = g.getElement("s6", Sequence.class);
        assertAll(
                () -> assertEquals(5, s6.getArgument(START_WITH)),
                () -> assertEquals(-1, s6.getArgument(INCREMENT_BY)),
                () -> assertEquals(4, s6.getArgument(MINVALUE)),
                () -> assertEquals(6, s6.getArgument(MAXVALUE)),
                () -> assertFalse(s6.hasArgument(CYCLE)),
                () -> assertEquals("TEST", s6.getCelestaDoc())
        );

        Sequence s7 = g.getElement("s7", Sequence.class);
        assertAll(
                () -> assertEquals(-3, s7.getArgument(START_WITH)),
                () -> assertEquals(-2, s7.getArgument(INCREMENT_BY)),
                () -> assertEquals(-6, s7.getArgument(MINVALUE)),
                () -> assertEquals(-3, s7.getArgument(MAXVALUE)),
                () -> assertFalse(s7.hasArgument(CYCLE))
        );

        Sequence s8 = g.getElement("s8", Sequence.class);
        assertAll(
                () -> assertEquals(-1, s8.getArgument(START_WITH)),
                () -> assertEquals(-2, s8.getArgument(INCREMENT_BY)),
                () -> assertEquals(-1, s8.getArgument(MINVALUE)),
                () -> assertEquals(2, s8.getArgument(MAXVALUE)),
                () -> assertFalse(s8.hasArgument(CYCLE))
        );

        Sequence s9 = g.getElement("s9", Sequence.class);
        assertAll(
                () -> assertEquals(5, s9.getArgument(START_WITH)),
                () -> assertEquals(1, s9.getArgument(INCREMENT_BY)),
                () -> assertEquals(5, s9.getArgument(MINVALUE)),
                () -> assertFalse(s9.hasArgument(MAXVALUE)),
                () -> assertFalse(s9.hasArgument(CYCLE))
        );

        Sequence s10 = g.getElement("s10", Sequence.class);
        assertAll(
                () -> assertEquals(-5, s10.getArgument(START_WITH)),
                () -> assertEquals(1, s10.getArgument(INCREMENT_BY)),
                () -> assertEquals(-5, s10.getArgument(MINVALUE)),
                () -> assertFalse(s10.hasArgument(MAXVALUE)),
                () -> assertFalse(s10.hasArgument(CYCLE))
        );

        Sequence s11 = g.getElement("s11", Sequence.class);
        assertAll(
                () -> assertEquals(1, s11.getArgument(START_WITH)),
                () -> assertEquals(1, s11.getArgument(INCREMENT_BY)),
                () -> assertFalse(s11.hasArgument(MINVALUE)),
                () -> assertEquals(56, s11.getArgument(MAXVALUE)),
                () -> assertFalse(s11.hasArgument(CYCLE))
        );

        Sequence s12 = g.getElement("s12", Sequence.class);
        assertAll(
                () -> assertEquals(1, s12.getArgument(START_WITH)),
                () -> assertEquals(1, s12.getArgument(INCREMENT_BY)),
                () -> assertFalse(s12.hasArgument(MINVALUE)),
                () -> assertEquals(5, s12.getArgument(MAXVALUE)),
                () -> assertEquals(true, s12.getArgument(CYCLE))
        );

        Sequence s13 = g.getElement("s13", Sequence.class);
        assertAll(
                () -> assertEquals(1, s13.getArgument(START_WITH)),
                () -> assertEquals(-1, s13.getArgument(INCREMENT_BY)),
                () -> assertEquals(-1, s13.getArgument(MINVALUE)),
                () -> assertEquals(1, s13.getArgument(MAXVALUE)),
                () -> assertEquals(true, s13.getArgument(CYCLE))
        );

        Sequence s14 = g.getElement("s14", Sequence.class);
        assertAll(
                () -> assertEquals(5, s14.getArgument(START_WITH)),
                () -> assertEquals(2, s14.getArgument(INCREMENT_BY)),
                () -> assertEquals(5, s14.getArgument(MINVALUE)),
                () -> assertEquals(56, s14.getArgument(MAXVALUE)),
                () -> assertEquals(true, s14.getArgument(CYCLE))
        );

        Sequence s15 = g.getElement("s14", Sequence.class);
        assertAll(
                () -> assertEquals(5, s15.getArgument(START_WITH)),
                () -> assertEquals(2, s15.getArgument(INCREMENT_BY)),
                () -> assertEquals(5, s15.getArgument(MINVALUE)),
                () -> assertEquals(56, s15.getArgument(MAXVALUE)),
                () -> assertEquals(true, s15.getArgument(CYCLE))
        );
    }

    @Test
    void testParsingFailsOnZeroIncrementBy() {
        ChecksumInputStream input = new ChecksumInputStream(
                ParserTest.class.getResourceAsStream(
                        "sequence/testParsingFailsOnZeroIncrementBy.sql"
                ));
        CelestaParser cp = new CelestaParser(input);
        assertThrows(ParseException.class, () -> cp.grain(s, "test"));
    }

    @Test
    void testParsingFailsWhenMinValueGtMaxValue() {
        ChecksumInputStream input = new ChecksumInputStream(
                ParserTest.class.getResourceAsStream(
                        "sequence/testParsingFailsWhenMinValueGtMaxValue.sql"
                ));
        CelestaParser cp = new CelestaParser(input);
        assertThrows(ParseException.class, () -> cp.grain(s, "test"));
    }

    @Test
    void testParsingFailsWhenMinValueGtStartWith() {
        ChecksumInputStream input = new ChecksumInputStream(
                ParserTest.class.getResourceAsStream(
                        "sequence/testParsingFailsWhenMinValueGtStartWith.sql"
                ));
        CelestaParser cp = new CelestaParser(input);
        assertThrows(ParseException.class, () -> cp.grain(s, "test"));
    }

    @Test
    void testParsingFailsWhenMaxValueLtStartWith() {
        ChecksumInputStream input = new ChecksumInputStream(
                ParserTest.class.getResourceAsStream(
                        "sequence/testParsingFailsWhenMaxValueLtStartWith.sql"
                ));
        CelestaParser cp = new CelestaParser(input);
        assertThrows(ParseException.class, () -> cp.grain(s, "test"));
    }

    @Test
    void testParsingFailsWhenIncrementIsPositiveAndCycleWithoutMaxValue() {
        ChecksumInputStream input = new ChecksumInputStream(
                ParserTest.class.getResourceAsStream(
                        "sequence/testParsingFailsWhenIncrementIsPositiveAndCycleWithoutMaxValue.sql"
                ));
        CelestaParser cp = new CelestaParser(input);
        assertThrows(ParseException.class, () -> cp.grain(s, "test"));
    }

    @Test
    void testParsingFailsWhenIncrementIsNegativeWithoutMaxValue() {
        ChecksumInputStream input = new ChecksumInputStream(
                ParserTest.class.getResourceAsStream(
                        "sequence/testParsingFailsWhenIncrementIsNegativeWithoutMaxValue.sql"
                ));
        CelestaParser cp = new CelestaParser(input);
        assertThrows(ParseException.class, () -> cp.grain(s, "test"));
    }

    @Test
    void testParsingFailsWhenIncIsNegativeAndSwIsPositiveAndSumOfSwAndIncLtMinValue() {
        ChecksumInputStream input = new ChecksumInputStream(
                ParserTest.class.getResourceAsStream(
                        "sequence/testParsingFailsWhenIncIsNegativeAndSwIsPositiveAndSumOfSwAndIncLtMinValue.sql"
                ));
        CelestaParser cp = new CelestaParser(input);
        assertThrows(ParseException.class, () -> cp.grain(s, "test"));
    }

    @Test
    void testParsingFailsWhenIncIsNegativeAndAbsOfIncGtOrEqAbsOfSubtractionOfMaxValueAndMinValue() {
        ChecksumInputStream input = new ChecksumInputStream(
                ParserTest.class.getResourceAsStream(
                        "sequence/testParsingFailsWhenIncIsNegativeAndAbsOfIncGtOrEqAbsOfSubtractionOfMaxValueAndMinValue.sql"
                ));
        CelestaParser cp = new CelestaParser(input);
        assertThrows(ParseException.class, () -> cp.grain(s, "test"));
    }

    @Test
    void testParsingFailsOnStartWithDuplication() {
        ChecksumInputStream input = new ChecksumInputStream(
                ParserTest.class.getResourceAsStream(
                        "sequence/testParsingFailsOnStartWithDuplication.sql"
                ));
        CelestaParser cp = new CelestaParser(input);
        assertThrows(ParseException.class, () -> cp.grain(s, "test"));
    }

    @Test
    void testParsingFailsOnIncrementByDuplication() {
        ChecksumInputStream input = new ChecksumInputStream(
                ParserTest.class.getResourceAsStream(
                        "sequence/testParsingFailsOnIncrementByDuplication.sql"
                ));
        CelestaParser cp = new CelestaParser(input);
        assertThrows(ParseException.class, () -> cp.grain(s, "test"));
    }

    @Test
    void testParsingFailsOnMinValueDuplication() {
        ChecksumInputStream input = new ChecksumInputStream(
                ParserTest.class.getResourceAsStream(
                        "sequence/testParsingFailsOnMinValueDuplication.sql"
                ));
        CelestaParser cp = new CelestaParser(input);
        assertThrows(ParseException.class, () -> cp.grain(s, "test"));
    }

    @Test
    void testParsingFailsOnMaxValueDuplication() {
        ChecksumInputStream input = new ChecksumInputStream(
                ParserTest.class.getResourceAsStream(
                        "sequence/testParsingFailsOnMaxValueDuplication.sql"
                ));
        CelestaParser cp = new CelestaParser(input);
        assertThrows(ParseException.class, () -> cp.grain(s, "test"));
    }

    @Test
    void testParsingFailsOnCycleDuplication() {
        ChecksumInputStream input = new ChecksumInputStream(
                ParserTest.class.getResourceAsStream(
                        "sequence/testParsingFailsOnCycleDuplication.sql"
                ));
        CelestaParser cp = new CelestaParser(input);
        assertThrows(ParseException.class, () -> cp.grain(s, "test"));
    }
}

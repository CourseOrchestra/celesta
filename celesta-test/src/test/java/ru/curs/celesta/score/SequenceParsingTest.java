package ru.curs.celesta.score;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static ru.curs.celesta.score.SequenceElement.Argument.*;

public class SequenceParsingTest extends AbstractParsingTest {

    @Test
    void testParsingOnCorrectSyntax() throws ParseException {
        ChecksumInputStream input = new ChecksumInputStream(
                ParserTest.class.getResourceAsStream(
                        "sequence/testParsingOnCorrectSyntax.sql"
                ));
        CelestaParser cp = new CelestaParser(input);
        Grain g = cp.grain(s, "test");

        SequenceElement s1 = g.getElement("s1", SequenceElement.class);
        assertAll(
                () -> assertEquals(1L, s1.getArgument(START_WITH)),
                () -> assertEquals(1L, s1.getArgument(INCREMENT_BY)),
                () -> assertEquals(1L, s1.getArgument(MINVALUE)),
                () -> assertEquals(Long.MAX_VALUE, s1.getArgument(MAXVALUE)),
                () -> assertEquals(false, s1.getArgument(CYCLE))
        );

        SequenceElement s2 = g.getElement("s2", SequenceElement.class);
        assertAll(
                () -> assertEquals(3L, s2.getArgument(START_WITH)),
                () -> assertEquals(1L, s2.getArgument(INCREMENT_BY)),
                () -> assertEquals(3L, s2.getArgument(MINVALUE)),
                () -> assertEquals(Long.MAX_VALUE, s2.getArgument(MAXVALUE)),
                () -> assertEquals(false, s2.getArgument(CYCLE))
        );

        SequenceElement s3 = g.getElement("s3", SequenceElement.class);
        assertAll(
                () -> assertEquals(1L, s3.getArgument(START_WITH)),
                () -> assertEquals(2L, s3.getArgument(INCREMENT_BY)),
                () -> assertEquals(1L, s3.getArgument(MINVALUE)),
                () -> assertEquals(Long.MAX_VALUE, s3.getArgument(MAXVALUE)),
                () -> assertEquals(false, s3.getArgument(CYCLE))
        );

        SequenceElement s4 = g.getElement("s4", SequenceElement.class);
        assertAll(
                () -> assertEquals(1L, s4.getArgument(START_WITH)),
                () -> assertEquals(-2L, s4.getArgument(INCREMENT_BY)),
                () -> assertEquals(-99999999L, s4.getArgument(MINVALUE)),
                () -> assertEquals(1L, s4.getArgument(MAXVALUE)),
                () -> assertEquals(false, s4.getArgument(CYCLE))
        );

        SequenceElement s5 = g.getElement("s5", SequenceElement.class);
        assertAll(
                () -> assertEquals(5L, s5.getArgument(START_WITH)),
                () -> assertEquals(-1L, s5.getArgument(INCREMENT_BY)),
                () -> assertEquals(-99999999L, s5.getArgument(MINVALUE)),
                () -> assertEquals(5L, s5.getArgument(MAXVALUE)),
                () -> assertEquals(false, s5.getArgument(CYCLE))
        );

        SequenceElement s6 = g.getElement("s6", SequenceElement.class);
        assertAll(
                () -> assertEquals(5L, s6.getArgument(START_WITH)),
                () -> assertEquals(-1L, s6.getArgument(INCREMENT_BY)),
                () -> assertEquals(4L, s6.getArgument(MINVALUE)),
                () -> assertEquals(6L, s6.getArgument(MAXVALUE)),
                () -> assertEquals(false, s6.getArgument(CYCLE)),
                () -> assertEquals("TEST", s6.getCelestaDoc())
        );

        SequenceElement s7 = g.getElement("s7", SequenceElement.class);
        assertAll(
                () -> assertEquals(-3L, s7.getArgument(START_WITH)),
                () -> assertEquals(-2L, s7.getArgument(INCREMENT_BY)),
                () -> assertEquals(-6L, s7.getArgument(MINVALUE)),
                () -> assertEquals(-3L, s7.getArgument(MAXVALUE)),
                () -> assertEquals(false, s7.getArgument(CYCLE))
        );

        SequenceElement s8 = g.getElement("s8", SequenceElement.class);
        assertAll(
                () -> assertEquals(-1L, s8.getArgument(START_WITH)),
                () -> assertEquals(-2L, s8.getArgument(INCREMENT_BY)),
                () -> assertEquals(-1L, s8.getArgument(MINVALUE)),
                () -> assertEquals(2L, s8.getArgument(MAXVALUE)),
                () -> assertEquals(false, s8.getArgument(CYCLE))
        );

        SequenceElement s9 = g.getElement("s9", SequenceElement.class);
        assertAll(
                () -> assertEquals(5L, s9.getArgument(START_WITH)),
                () -> assertEquals(1L, s9.getArgument(INCREMENT_BY)),
                () -> assertEquals(5l, s9.getArgument(MINVALUE)),
                () -> assertEquals(Long.MAX_VALUE, s9.getArgument(MAXVALUE)),
                () -> assertEquals(false, s9.getArgument(CYCLE))
        );

        SequenceElement s10 = g.getElement("s10", SequenceElement.class);
        assertAll(
                () -> assertEquals(-5L, s10.getArgument(START_WITH)),
                () -> assertEquals(1L, s10.getArgument(INCREMENT_BY)),
                () -> assertEquals(-5L, s10.getArgument(MINVALUE)),
                () -> assertEquals(Long.MAX_VALUE, s10.getArgument(MAXVALUE)),
                () -> assertEquals(false, s10.getArgument(CYCLE))
        );

        SequenceElement s11 = g.getElement("s11", SequenceElement.class);
        assertAll(
                () -> assertEquals(1L, s11.getArgument(START_WITH)),
                () -> assertEquals(1L, s11.getArgument(INCREMENT_BY)),
                () -> assertEquals(1L, s11.getArgument(MINVALUE)),
                () -> assertEquals(56L, s11.getArgument(MAXVALUE)),
                () -> assertEquals(false, s11.getArgument(CYCLE))
        );

        SequenceElement s12 = g.getElement("s12", SequenceElement.class);
        assertAll(
                () -> assertEquals(1L, s12.getArgument(START_WITH)),
                () -> assertEquals(1L, s12.getArgument(INCREMENT_BY)),
                () -> assertEquals(1L, s12.getArgument(MINVALUE)),
                () -> assertEquals(5L, s12.getArgument(MAXVALUE)),
                () -> assertEquals(true, s12.getArgument(CYCLE))
        );

        SequenceElement s13 = g.getElement("s13", SequenceElement.class);
        assertAll(
                () -> assertEquals(1L, s13.getArgument(START_WITH)),
                () -> assertEquals(-1L, s13.getArgument(INCREMENT_BY)),
                () -> assertEquals(-1L, s13.getArgument(MINVALUE)),
                () -> assertEquals(1L, s13.getArgument(MAXVALUE)),
                () -> assertEquals(true, s13.getArgument(CYCLE))
        );

        SequenceElement s14 = g.getElement("s14", SequenceElement.class);
        assertAll(
                () -> assertEquals(5L, s14.getArgument(START_WITH)),
                () -> assertEquals(2L, s14.getArgument(INCREMENT_BY)),
                () -> assertEquals(5L, s14.getArgument(MINVALUE)),
                () -> assertEquals(56L, s14.getArgument(MAXVALUE)),
                () -> assertEquals(true, s14.getArgument(CYCLE))
        );

        SequenceElement s15 = g.getElement("s15", SequenceElement.class);
        assertAll(
                () -> assertEquals(5L, s15.getArgument(START_WITH)),
                () -> assertEquals(2L, s15.getArgument(INCREMENT_BY)),
                () -> assertEquals(5L, s15.getArgument(MINVALUE)),
                () -> assertEquals(56L, s15.getArgument(MAXVALUE)),
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

    @Test
    void testParsingFailsWhenSequenceIdentifierIsReservedByCelesta() {
        ChecksumInputStream input = new ChecksumInputStream(
                ParserTest.class.getResourceAsStream(
                        "sequence/testParsingFailsWhenSequenceIdentifierIsReservedByCelesta.sql"
                ));
        CelestaParser cp = new CelestaParser(input);
        assertThrows(ParseException.class, () -> cp.grain(s, "test"));
    }
}

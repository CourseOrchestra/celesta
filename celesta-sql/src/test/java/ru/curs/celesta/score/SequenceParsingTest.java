package ru.curs.celesta.score;

import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;
import static ru.curs.celesta.score.SequenceElement.Argument.CYCLE;
import static ru.curs.celesta.score.SequenceElement.Argument.INCREMENT_BY;
import static ru.curs.celesta.score.SequenceElement.Argument.MAXVALUE;
import static ru.curs.celesta.score.SequenceElement.Argument.MINVALUE;
import static ru.curs.celesta.score.SequenceElement.Argument.START_WITH;

public class SequenceParsingTest extends AbstractParsingTest {

    @Test
    void testParsingOnCorrectSyntax() throws Exception {
        File f = ResourceUtil.getResourceAsFile(
                ParserTest.class,
                "sequence/testParsingOnCorrectSyntax.sql"
        );
        Grain g = parse(f);

        SequenceElement s1 = g.getElement("s1", SequenceElement.class);
        assertAll(
                () -> assertEquals(1L, s1.getStartWith()),
                () -> assertEquals(1L, s1.getIncrementBy()),
                () -> assertEquals(1L, s1.getMinValue()),
                () -> assertEquals(Long.MAX_VALUE, s1.getMaxValue()),
                () -> assertEquals(false, s1.isCycle())
        );

        SequenceElement s2 = g.getElement("s2", SequenceElement.class);
        assertAll(
                () -> assertEquals(3L, s2.getStartWith()),
                () -> assertEquals(1L, s2.getIncrementBy()),
                () -> assertEquals(3L, s2.getMinValue()),
                () -> assertEquals(Long.MAX_VALUE, s2.getMaxValue()),
                () -> assertEquals(false, s2.isCycle())
        );

        SequenceElement s3 = g.getElement("s3", SequenceElement.class);
        assertAll(
                () -> assertEquals(1L, s3.getStartWith()),
                () -> assertEquals(2L, s3.getIncrementBy()),
                () -> assertEquals(1L, s3.getMinValue()),
                () -> assertEquals(Long.MAX_VALUE, s3.getMaxValue()),
                () -> assertEquals(false, s3.isCycle())
        );

        SequenceElement s4 = g.getElement("s4", SequenceElement.class);
        assertAll(
                () -> assertEquals(1L, s4.getStartWith()),
                () -> assertEquals(-2L, s4.getIncrementBy()),
                () -> assertEquals(-99999999L, s4.getMinValue()),
                () -> assertEquals(1L, s4.getMaxValue()),
                () -> assertEquals(false, s4.isCycle())
        );

        SequenceElement s5 = g.getElement("s5", SequenceElement.class);
        assertAll(
                () -> assertEquals(5L, s5.getStartWith()),
                () -> assertEquals(-1L, s5.getIncrementBy()),
                () -> assertEquals(-99999999L, s5.getMinValue()),
                () -> assertEquals(5L, s5.getMaxValue()),
                () -> assertEquals(false, s5.isCycle())
        );

        SequenceElement s6 = g.getElement("s6", SequenceElement.class);
        assertAll(
                () -> assertEquals(5L, s6.getStartWith()),
                () -> assertEquals(-1L, s6.getIncrementBy()),
                () -> assertEquals(4L, s6.getMinValue()),
                () -> assertEquals(6L, s6.getMaxValue()),
                () -> assertEquals(false, s6.isCycle()),
                () -> assertEquals("TEST", s6.getCelestaDoc())
        );

        SequenceElement s7 = g.getElement("s7", SequenceElement.class);
        assertAll(
                () -> assertEquals(-3L, s7.getStartWith()),
                () -> assertEquals(-2L, s7.getIncrementBy()),
                () -> assertEquals(-6L, s7.getMinValue()),
                () -> assertEquals(-3L, s7.getMaxValue()),
                () -> assertEquals(false, s7.isCycle())
        );

        SequenceElement s8 = g.getElement("s8", SequenceElement.class);
        assertAll(
                () -> assertEquals(-1L, s8.getStartWith()),
                () -> assertEquals(-2L, s8.getIncrementBy()),
                () -> assertEquals(-1L, s8.getMinValue()),
                () -> assertEquals(2L, s8.getMaxValue()),
                () -> assertEquals(false, s8.isCycle())
        );

        SequenceElement s9 = g.getElement("s9", SequenceElement.class);
        assertAll(
                () -> assertEquals(5L, s9.getStartWith()),
                () -> assertEquals(1L, s9.getIncrementBy()),
                () -> assertEquals(5L, s9.getMinValue()),
                () -> assertEquals(Long.MAX_VALUE, s9.getMaxValue()),
                () -> assertEquals(false, s9.isCycle())
        );

        SequenceElement s10 = g.getElement("s10", SequenceElement.class);
        assertAll(
                () -> assertEquals(-5L, s10.getStartWith()),
                () -> assertEquals(1L, s10.getIncrementBy()),
                () -> assertEquals(-5L, s10.getMinValue()),
                () -> assertEquals(Long.MAX_VALUE, s10.getMaxValue()),
                () -> assertEquals(false, s10.isCycle())
        );

        SequenceElement s11 = g.getElement("s11", SequenceElement.class);
        assertAll(
                () -> assertEquals(1L, s11.getStartWith()),
                () -> assertEquals(1L, s11.getIncrementBy()),
                () -> assertEquals(1L, s11.getMinValue()),
                () -> assertEquals(56L, s11.getMaxValue()),
                () -> assertEquals(false, s11.isCycle())
        );

        SequenceElement s12 = g.getElement("s12", SequenceElement.class);
        assertAll(
                () -> assertEquals(1L, s12.getStartWith()),
                () -> assertEquals(1L, s12.getIncrementBy()),
                () -> assertEquals(1L, s12.getMinValue()),
                () -> assertEquals(5L, s12.getMaxValue()),
                () -> assertEquals(true, s12.isCycle())
        );

        SequenceElement s13 = g.getElement("s13", SequenceElement.class);
        assertAll(
                () -> assertEquals(1L, s13.getStartWith()),
                () -> assertEquals(-1L, s13.getIncrementBy()),
                () -> assertEquals(-1L, s13.getMinValue()),
                () -> assertEquals(1L, s13.getMaxValue()),
                () -> assertEquals(true, s13.isCycle())
        );

        SequenceElement s14 = g.getElement("s14", SequenceElement.class);
        assertAll(
                () -> assertEquals(5L, s14.getStartWith()),
                () -> assertEquals(2L, s14.getIncrementBy()),
                () -> assertEquals(5L, s14.getMinValue()),
                () -> assertEquals(56L, s14.getMaxValue()),
                () -> assertEquals(true, s14.isCycle())
        );

        SequenceElement s15 = g.getElement("s15", SequenceElement.class);
        assertAll(
                () -> assertEquals(5L, s15.getStartWith()),
                () -> assertEquals(2L, s15.getIncrementBy()),
                () -> assertEquals(5L, s15.getMinValue()),
                () -> assertEquals(56L, s15.getMaxValue()),
                () -> assertEquals(true, s15.isCycle())
        );
    }

    @Test
    void testParsingFailsOnZeroIncrementBy() {
        File f = ResourceUtil.getResourceAsFile(
                ParserTest.class,
                "sequence/testParsingFailsOnZeroIncrementBy.sql"
        );
        assertThrows(ParseException.class, () -> parse(f));
    }

    @Test
    void testParsingFailsWhenMinValueGtMaxValue() {
        File f = ResourceUtil.getResourceAsFile(
                ParserTest.class,
                "sequence/testParsingFailsWhenMinValueGtMaxValue.sql"
        );
        assertThrows(ParseException.class, () -> parse(f));
    }

    @Test
    void testParsingFailsWhenMinValueGtStartWith() {
        File f = ResourceUtil.getResourceAsFile(
                ParserTest.class,
                "sequence/testParsingFailsWhenMinValueGtStartWith.sql"
        );
        assertThrows(ParseException.class, () -> parse(f));
    }

    @Test
    void testParsingFailsWhenMaxValueLtStartWith() {
        File f = ResourceUtil.getResourceAsFile(
                ParserTest.class,
                "sequence/testParsingFailsWhenMaxValueLtStartWith.sql"
        );
        assertThrows(ParseException.class, () -> parse(f));
    }

    @Test
    void testParsingFailsWhenIncIsNegativeAndSwIsPositiveAndSumOfSwAndIncLtMinValue() {
        File f = ResourceUtil.getResourceAsFile(
                ParserTest.class,
                "sequence/testParsingFailsWhenIncIsNegativeAndSwIsPositiveAndSumOfSwAndIncLtMinValue.sql"
        );
        assertThrows(ParseException.class, () -> parse(f));
    }

    @Test
    void testParsingFailsWhenIncIsNegativeAndAbsOfIncGtOrEqAbsOfSubtractionOfMaxValueAndMinValue() {
        File f = ResourceUtil.getResourceAsFile(
                ParserTest.class,
                "sequence/testParsingFailsWhenIncIsNegativeAndAbsOfIncGtOrEqAbsOfSubtractionOfMaxValueAndMinValue.sql"
        );
        assertThrows(ParseException.class, () -> parse(f));
    }

    @Test
    void testParsingFailsOnStartWithDuplication() {
        File f = ResourceUtil.getResourceAsFile(
                ParserTest.class,
                "sequence/testParsingFailsOnStartWithDuplication.sql"
        );
        assertThrows(ParseException.class, () -> parse(f));
    }

    @Test
    void testParsingFailsOnIncrementByDuplication() {
        File f = ResourceUtil.getResourceAsFile(
                ParserTest.class,
                "sequence/testParsingFailsOnIncrementByDuplication.sql"
        );
        assertThrows(ParseException.class, () -> parse(f));
    }

    @Test
    void testParsingFailsOnMinValueDuplication() {
        File f = ResourceUtil.getResourceAsFile(
                ParserTest.class,
                "sequence/testParsingFailsOnMinValueDuplication.sql"
        );
        assertThrows(ParseException.class, () -> parse(f));
    }

    @Test
    void testParsingFailsOnMaxValueDuplication() {
        File f = ResourceUtil.getResourceAsFile(
                ParserTest.class,
                "sequence/testParsingFailsOnMaxValueDuplication.sql"
        );
        assertThrows(ParseException.class, () -> parse(f));
    }

    @Test
    void testParsingFailsOnCycleDuplication() {
        File f = ResourceUtil.getResourceAsFile(
                ParserTest.class,
                "sequence/testParsingFailsOnCycleDuplication.sql"
        );
        assertThrows(ParseException.class, () -> parse(f));
    }

    @Test
    void testParsingFailsWhenSequenceIdentifierIsReservedByCelesta() {
        File f = ResourceUtil.getResourceAsFile(
                ParserTest.class,
                "sequence/testParsingFailsWhenSequenceIdentifierIsReservedByCelesta.sql"
        );
        assertThrows(ParseException.class, () -> parse(f));
    }
}

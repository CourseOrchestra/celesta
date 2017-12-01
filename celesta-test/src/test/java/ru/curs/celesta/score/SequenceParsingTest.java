package ru.curs.celesta.score;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

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
                () -> assertEquals(0, s1.getStartWith()),
                () -> assertEquals(1, s1.getIncrementBy()),
                () -> assertFalse(s1.getMinValue().isPresent()),
                () -> assertFalse(s1.getMaxValue().isPresent()),
                () -> assertFalse(s1.getIsCycle().isPresent())
        );

        Sequence s2 = g.getElement("s2", Sequence.class);
        assertAll(
                () -> assertEquals(3, s2.getStartWith()),
                () -> assertEquals(1, s2.getIncrementBy()),
                () -> assertFalse(s2.getMinValue().isPresent()),
                () -> assertFalse(s2.getMaxValue().isPresent()),
                () -> assertFalse(s2.getIsCycle().isPresent())
        );

        Sequence s3 = g.getElement("s3", Sequence.class);
        assertAll(
                () -> assertEquals(0, s3.getStartWith()),
                () -> assertEquals(2, s3.getIncrementBy()),
                () -> assertFalse(s3.getMinValue().isPresent()),
                () -> assertFalse(s3.getMaxValue().isPresent()),
                () -> assertFalse(s3.getIsCycle().isPresent())
        );

        Sequence s4 = g.getElement("s4", Sequence.class);
        assertAll(
                () -> assertEquals(0, s4.getStartWith()),
                () -> assertEquals(1, s4.getIncrementBy()),
                () -> assertEquals(5, s4.getMinValue()),
                () -> assertFalse(s4.getMaxValue().isPresent()),
                () -> assertFalse(s4.getIsCycle().isPresent())
        );

        Sequence s5 = g.getElement("s5", Sequence.class);
        assertAll(
                () -> assertEquals(0, s5.getStartWith()),
                () -> assertEquals(1, s5.getIncrementBy()),
                () -> assertFalse(s5.getMinValue().isPresent()),
                () -> assertEquals(56, s5.getMaxValue()),
                () -> assertFalse(s5.getIsCycle().isPresent())
        );

        Sequence s6 = g.getElement("s6", Sequence.class);
        assertAll(
                () -> assertEquals(0, s6.getStartWith()),
                () -> assertEquals(1, s6.getIncrementBy()),
                () -> assertFalse(s6.getMinValue().isPresent()),
                () -> assertFalse(s6.getMaxValue().isPresent()),
                () -> assertTrue(s6.getIsCycle().get())
        );

        Sequence s7 = g.getElement("s", Sequence.class);
        assertAll(
                () -> assertEquals(3, s7.getStartWith()),
                () -> assertEquals(2, s7.getIncrementBy()),
                () -> assertEquals(5, s7.getMinValue()),
                () -> assertEquals(56, s7.getMaxValue()),
                () -> assertTrue(s7.getIsCycle().get())
        );

        Sequence s8 = g.getElement("s", Sequence.class);
        assertAll(
                () -> assertEquals(3, s8.getStartWith()),
                () -> assertEquals(2, s8.getIncrementBy()),
                () -> assertEquals(5, s8.getMinValue()),
                () -> assertEquals(56, s8.getMaxValue()),
                () -> assertTrue(s8.getIsCycle().get())
        );
    }

    @Test
    void testParsingFailsOnZeroIncrementBy() throws ParseException {
        ChecksumInputStream input = new ChecksumInputStream(
                ParserTest.class.getResourceAsStream(
                        "sequence/testParsingFailsOnZeroIncrementBy.sql"
                ));
        CelestaParser cp = new CelestaParser(input);
        assertThrows(ParseException.class, () -> cp.grain(s, "test"));
    }

    @Test
    void testParsingFailsWhenMinValueGtMaxValue() throws ParseException {
        ChecksumInputStream input = new ChecksumInputStream(
                ParserTest.class.getResourceAsStream(
                        "sequence/testParsingFailsWhenMinValueGtMaxValue.sql"
                ));
        CelestaParser cp = new CelestaParser(input);
        assertThrows(ParseException.class, () -> cp.grain(s, "test"));
    }
}

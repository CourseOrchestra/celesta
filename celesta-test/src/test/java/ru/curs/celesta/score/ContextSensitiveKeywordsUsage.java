package ru.curs.celesta.score;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class ContextSensitiveKeywordsUsage extends  AbstractParsingTest{
    @Test
    void testParsingOnCorrectSyntax() throws ParseException {
        ChecksumInputStream input = new ChecksumInputStream(
                ParserTest.class.getResourceAsStream(
                        "lexerstates/testContextSensitiveKeywordsUsage.sql"
                ));
        CelestaParser cp = new CelestaParser(input);
        Grain g = cp.grain(s, "test");

        Table foo = g.getElement("foo", Table.class);
        assertArrayEquals(new String[]{"CYCLE", "MAXVALUE", "MINVALUE", "INCREMENT",
                "VERSION", "GRAIN"},
                foo.getColumns().keySet().toArray(new String[]{}));
    }
}

package ru.curs.celesta.score;

import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class ContextSensitiveKeywordsUsage extends  AbstractParsingTest{
    @Test
    void testParsingOnCorrectSyntax() throws Exception {
        File f = ResourceUtil.getResourceAsFile(
                ParserTest.class,
                "lexerstates/testContextSensitiveKeywordsUsage.sql"
        );
        Grain g = parse(f);

        Table foo = g.getElement("foo", Table.class);
        assertArrayEquals(new String[]{"CYCLE", "MAXVALUE", "MINVALUE", "INCREMENT",
                "VERSION", "GRAIN", "AUTOUPDATE", "READ", "ONLY"},
                foo.getColumns().keySet().toArray(new String[]{}));
    }
}

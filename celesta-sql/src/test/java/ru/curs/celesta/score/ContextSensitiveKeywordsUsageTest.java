package ru.curs.celesta.score;

import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class ContextSensitiveKeywordsUsageTest extends  AbstractParsingTest{
    @Test
    void testParsingOnCorrectSyntax() throws Exception {
        File f = ResourceUtil.getResourceAsFile(
                ParserTest.class,
                "lexerstates/testContextSensitiveKeywordsUsage.sql"
        );
        Grain g = parse(f);

        BasicTable foo = g.getElement("foo", BasicTable.class);
        assertArrayEquals(new String[]{"CYCLE", "MAXVALUE", "MINVALUE", "INCREMENT",
                "VERSION", "GRAIN", "AUTOUPDATE", "READ", "ONLY", "ZONE", "TIME"},
                foo.getColumns().keySet().toArray(new String[]{}));
    }
}

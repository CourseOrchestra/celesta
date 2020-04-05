package ru.curs.celesta.score;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled("Subquery not implemented yet")
public class SubqueryViewParsingTest extends AbstractParsingTest {

    @Test
    public void testParsingNotFailsWhenUnionAllSyntaxIsCorrect() throws Exception {
        File f = ResourceUtil.getResourceAsFile(
                ParserTest.class,
                "subquery/testParsingNotFailsWhenSubquerySyntaxIsCorrect.sql"
        );
        Grain g = parse(f);

    }

    @Test
    public void testParsingFailsWhenColumnNumberDiffers() {
        File f = ResourceUtil.getResourceAsFile(
                ParserTest.class,
                "subquery/testParsingFailsWhenColumnNumberDiffers.sql"
        );
        assertTrue(assertThrows(ParseException.class, () -> parse(f)).getMessage().contains("differs"));
    }

  @Test
  public void testParsingFailsWhenColumnTypesDiffer() {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "subquery/testParsingFailsWhenColumnTypesDiffer.sql"
    );
    assertTrue(assertThrows(ParseException.class, () -> parse(f)).getMessage().contains("mismatch"));
  }
}



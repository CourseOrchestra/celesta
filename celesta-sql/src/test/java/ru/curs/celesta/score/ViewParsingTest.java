package ru.curs.celesta.score;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

import java.io.File;

/**
 * Created by ioann on 15.06.2017.
 */
public class ViewParsingTest extends AbstractParsingTest {

  @Test
  public void testParsingFailsWhenStringInSum() {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "view/testParsingFailsWhenStringInSum.sql"
    );
    assertThrows(ParseException.class, () -> parse(f));
  }

  @Test
  public void testParsingFailsWhenGroupByContainsNotAllFieldRefs() {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "view/testParsingFailsWhenGroupByContainsNotAllFieldRefs.sql"
    );
    assertThrows(ParseException.class, () -> parse(f));
  }

  @Test
  public void testParsingFailsWhenAggregateExprInGroupBy() {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "view/testParsingFailsWhenAggregateExprInGroupBy.sql"
    );
    assertThrows(ParseException.class, () -> parse(f));
  }

  @Test
  public void testParsingNotFailsWhenViewSyntaxIsCorrect() throws Exception {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "view/testParsingNotFailsWhenViewSyntaxIsCorrect.sql"
    );
    parse(f);
  }

  @Test
  public void testParsingFailsWhenParameterIsUsed() {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "view/testParsingFailsWhenParameterIsUsed.sql"
    );
    assertThrows(ParseException.class, () -> parse(f));
  }
}

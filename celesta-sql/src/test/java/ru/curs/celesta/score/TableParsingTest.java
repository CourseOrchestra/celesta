package ru.curs.celesta.score;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

import java.io.File;

/**
 * Created by ioann on 11.09.2017.
 */
public class TableParsingTest extends AbstractParsingTest {

  @Test
  public void testParsingFailsWhenSinglePkAndIndexMatches() throws ParseException {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "table/testParsingFailsWhenSinglePkAndIndexMatches.sql"
    );
    assertThrows(ParseException.class, () -> parse(f));
  }

  @Test
  public void testParsingFailsWhenComplexPkAndIndexMatches() throws ParseException {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "table/testParsingFailsWhenComplexPkAndIndexMatches.sql"
    );
    assertThrows(ParseException.class, () -> parse(f));
  }

  @Test
  public void testParsingNotFailsWhenComplexPkAndIndexMatchesByFieldsButNotByOrder() throws Exception {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "table/testParsingNotFailsWhenComplexPkAndIndexMatchesByFieldsButNotByOrder.sql"
    );
    parse(f);
  }

  @Test
  public void testOptions() throws Exception{
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "table/testTableOptionsCombinations.sql"
    );
    Grain g = parse(f);
    Table t = g.getTable("t1");
    assertTrue(t.isReadOnly());
    assertTrue(t.isAutoUpdate());
    assertFalse(t.isVersioned());

    t = g.getTable("t2");
    assertFalse(t.isReadOnly());
    assertTrue(t.isAutoUpdate());
    assertFalse(t.isVersioned());

    t = g.getTable("t3");
    assertFalse(t.isReadOnly());
    assertFalse(t.isAutoUpdate());
    assertTrue(t.isVersioned());

    t = g.getTable("t4");
    assertFalse(t.isReadOnly());
    assertFalse(t.isAutoUpdate());
    assertFalse(t.isVersioned());

    t = g.getTable("t5");
    assertFalse(t.isReadOnly());
    assertTrue(t.isAutoUpdate());
    assertTrue(t.isVersioned());
  }

}

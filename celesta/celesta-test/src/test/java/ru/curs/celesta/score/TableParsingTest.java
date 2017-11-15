package ru.curs.celesta.score;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

/**
 * Created by ioann on 11.09.2017.
 */
public class TableParsingTest extends AbstractParsingTest {

  @Test
  public void testParsingFailsWhenSinglePkAndIndexMatches() throws ParseException {
    ChecksumInputStream input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream("table/testParsingFailsWhenSinglePkAndIndexMatches.sql"));
    CelestaParser cp = new CelestaParser(input);
    assertThrows(ParseException.class, () -> cp.grain(s, "test"));
  }

  @Test
  public void testParsingFailsWhenComplexPkAndIndexMatches() throws ParseException {
    ChecksumInputStream input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream("table/testParsingFailsWhenComplexPkAndIndexMatches.sql"));
    CelestaParser cp = new CelestaParser(input);
    assertThrows(ParseException.class, () -> cp.grain(s, "test"));
  }

  @Test
  public void testParsingNotFailsWhenComplexPkAndIndexMatchesByFieldsButNotByOrder() throws ParseException {
    ChecksumInputStream input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream(
            "table/testParsingNotFailsWhenComplexPkAndIndexMatchesByFieldsButNotByOrder.sql"
        ));
    CelestaParser cp = new CelestaParser(input);
    cp.grain(s, "test");
  }

}

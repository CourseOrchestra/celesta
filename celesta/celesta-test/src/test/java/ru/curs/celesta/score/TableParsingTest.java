package ru.curs.celesta.score;

import org.junit.Test;

/**
 * Created by ioann on 11.09.2017.
 */
public class TableParsingTest extends AbstractParsingTest {

  @Test(expected = ParseException.class)
  public void testParsingFailsWhenSinglePkAndIndexMatches() throws ParseException {
    ChecksumInputStream input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream("table/testParsingFailsWhenSinglePkAndIndexMatches.sql"));
    CelestaParser cp = new CelestaParser(input);
    cp.grain(s, "test");
  }

  @Test(expected = ParseException.class)
  public void testParsingFailsWhenComplexPkAndIndexMatches() throws ParseException {
    ChecksumInputStream input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream("table/testParsingFailsWhenComplexPkAndIndexMatches.sql"));
    CelestaParser cp = new CelestaParser(input);
    cp.grain(s, "test");
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

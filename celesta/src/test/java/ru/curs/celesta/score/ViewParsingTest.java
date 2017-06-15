package ru.curs.celesta.score;

import org.junit.Test;

/**
 * Created by ioann on 15.06.2017.
 */
public class ViewParsingTest extends AbstractParsingTest {

  @Test(expected = ParseException.class)
  public void testParsingFailsWhenStringInSum() throws ParseException {
    ChecksumInputStream input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream("view/testParsingFailsWhenStringInSum.sql"));
    CelestaParser cp = new CelestaParser(input);
    cp.grain(s, "test");
  }

  @Test(expected = ParseException.class)
  public void testParsingFailsWhenGroupByContainsNotAllFieldRefs() throws ParseException {
    ChecksumInputStream input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream("view/testParsingFailsWhenGroupByContainsNotAllFieldRefs.sql"));
    CelestaParser cp = new CelestaParser(input);
    cp.grain(s, "test");
  }

  @Test(expected = ParseException.class)
  public void testParsingFailsWhenAggregateExprInGroupBy() throws ParseException {
    ChecksumInputStream input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream("view/testParsingFailsWhenAggregateExprInGroupBy.sql"));
    CelestaParser cp = new CelestaParser(input);
    cp.grain(s, "test");
  }

  @Test
  public void testParsingNotFailsWhenViewSyntaxIsCorrect() throws ParseException {
    ChecksumInputStream input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream("view/testParsingNotFailsWhenViewSyntaxIsCorrect.sql"));
    CelestaParser cp = new CelestaParser(input);
    cp.grain(s, "test");
  }
}

package ru.curs.celesta.score;

import org.junit.Test;

/**
 * Created by ioann on 15.06.2017.
 */
public class MaterializedViewParsingTest extends AbstractParsingTest {

  @Test(expected = ParseException.class)
  public void testParsingFailsWhenNullableColumnInGroupBy() throws ParseException {
    ChecksumInputStream input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream("materializedView/testParsingFailsWhenNullableColumnInGroupBy.sql"));
    CelestaParser cp = new CelestaParser(input);
    cp.grain(s, "test");
  }

  @Test(expected = ParseException.class)
  public void testParsingFailsWithNoAggregateColumn() throws ParseException {
    ChecksumInputStream input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream("materializedView/testParsingFailsWithNoAggregateColumn.sql"));
    CelestaParser cp = new CelestaParser(input);
    cp.grain(s, "test");
  }

  @Test(expected = ParseException.class)
  public void testParsingFailsWithWhereCondition() throws ParseException {
    ChecksumInputStream input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream("materializedView/testParsingFailsWithWhereCondition.sql"));
    CelestaParser cp = new CelestaParser(input);
    cp.grain(s, "test");
  }

  @Test
  public void testParsingNotFailsWhenMaterializedViewSyntaxIsCorrect() throws ParseException {
    ChecksumInputStream input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream("materializedView/testParsingNotFailsWhenMaterializedViewSyntaxIsCorrect.sql"));
    CelestaParser cp = new CelestaParser(input);
    cp.grain(s, "test");
  }

}

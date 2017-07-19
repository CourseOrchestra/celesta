package ru.curs.celesta.score;

import static org.junit.Assert.*;
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
    Grain g = cp.grain(s, "test");

    assertEquals(3, g.getMaterializedViews().size());
    MaterializedView mv = g.getMaterializedView("testView1");

    Column c = mv.getColumn("sumv");
    assertEquals(IntegerColumn.CELESTA_TYPE, c.getCelestaType());
    Expr expr = mv.getAggregateColumns().get(c.getName());
    assertTrue(expr instanceof Sum);

    c = mv.getColumn("f3");
    assertTrue(c instanceof StringColumn);
    StringColumn stringColumn = (StringColumn) c;
    assertEquals(2, stringColumn.getLength());
  }


  @Test(expected = ParseException.class)
  public void testParsingFailsWhenRefTableInAnotherGrain() throws ParseException {
    ChecksumInputStream input;
    CelestaParser cp;

    try {
      //Парсим рабочую гранулу
      input = new ChecksumInputStream(
          ParserTest.class.getResourceAsStream("materializedView/testParsingNotFailsWhenMaterializedViewSyntaxIsCorrect.sql"));
      cp = new CelestaParser(input);
      cp.grain(s, "test");
    } catch (Exception e) {
      //Этот участок рабочий и проверяется в другом тесте.
      throw new RuntimeException(e);
    }

    //Пытаемся распарсить гранулу, в которой MaterializedView ссылается на таблицу из первой
    input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream("materializedView/testParsingFailsWhenRefTableInAnotherGrain.sql"));
    cp = new CelestaParser(input);
    cp.grain(s, "test2");
  }

}

package ru.curs.celesta.score;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

/**
 * Created by ioann on 15.06.2017.
 */
public class MaterializedViewParsingTest extends AbstractParsingTest {

  @Test
  public void testParsingFailsWhenNullableColumnInGroupBy() throws ParseException {
    ChecksumInputStream input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream("materializedView/testParsingFailsWhenNullableColumnInGroupBy.sql"));
    CelestaParser cp = new CelestaParser(input);
    assertThrows(ParseException.class, () -> cp.grain(s, "test"));
  }

  @Test
  public void testParsingFailsWithNoAggregateColumn() throws ParseException {
    ChecksumInputStream input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream("materializedView/testParsingFailsWithNoAggregateColumn.sql"));
    CelestaParser cp = new CelestaParser(input);
    assertThrows(ParseException.class, () -> cp.grain(s, "test"));
  }

  @Test
  public void testParsingFailsWithWhereCondition() throws ParseException {
    ChecksumInputStream input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream("materializedView/testParsingFailsWithWhereCondition.sql"));
    CelestaParser cp = new CelestaParser(input);
    assertThrows(ParseException.class, () ->  cp.grain(s, "test"));
  }

  @Test
  public void testParsingNotFailsWhenMaterializedViewSyntaxIsCorrect() throws ParseException {
    ChecksumInputStream input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream("materializedView/testParsingNotFailsWhenMaterializedViewSyntaxIsCorrect.sql"));
    CelestaParser cp = new CelestaParser(input);
    Grain g = cp.grain(s, "test");

    assertEquals(3, g.getElements(MaterializedView.class).size());
    MaterializedView mv = g.getElement("testView1", MaterializedView.class);

    Column c = mv.getColumn("sumv");
    assertEquals(IntegerColumn.CELESTA_TYPE, c.getCelestaType());
    Expr expr = mv.getAggregateColumns().get(c.getName());
    assertTrue(expr instanceof Sum);

    c = mv.getColumn("f3");
    assertTrue(c instanceof StringColumn);
    StringColumn stringColumn = (StringColumn) c;
    assertEquals(2, stringColumn.getLength());
  }


  @Test
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
    CelestaParser cp2 = new CelestaParser(input);
    assertThrows(ParseException.class, () -> cp2.grain(s, "test2"));
  }

  @Test
  public void testParsingFailsWithDateInAggregate() throws ParseException {
    ChecksumInputStream input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream("materializedView/testParsingFailsWithDateInAggregate.sql"));
    CelestaParser cp = new CelestaParser(input);
    assertThrows(ParseException.class, () -> cp.grain(s, "test"));
  }

}

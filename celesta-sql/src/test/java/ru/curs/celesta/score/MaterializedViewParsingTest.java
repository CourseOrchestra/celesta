package ru.curs.celesta.score;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

import java.io.File;

/**
 * Created by ioann on 15.06.2017.
 */
public class MaterializedViewParsingTest extends AbstractParsingTest {

  @Test
  public void testParsingFailsWhenNullableColumnInGroupBy() {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "materializedView/testParsingFailsWhenNullableColumnInGroupBy.sql"
    );
    assertThrows(ParseException.class, () -> parse(f));
  }

  @Test
  public void testParsingFailsWithNoAggregateColumn() {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "materializedView/testParsingFailsWithNoAggregateColumn.sql"
    );
    assertThrows(ParseException.class, () -> parse(f));
  }

  @Test
  public void testParsingFailsWithWhereCondition() {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "materializedView/testParsingFailsWithWhereCondition.sql"
    );
    assertThrows(ParseException.class, () ->  parse(f));
  }

  @Test
  public void testParsingNotFailsWhenMaterializedViewSyntaxIsCorrect() throws Exception {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "materializedView/testParsingNotFailsWhenMaterializedViewSyntaxIsCorrect.sql"
    );
    Grain g = parse(f);

    assertEquals(3, g.getElements(MaterializedView.class).size());
    MaterializedView mv = g.getElement("testView1", MaterializedView.class);

    Column<?> c = mv.getColumn("sumv");
    assertEquals(IntegerColumn.CELESTA_TYPE, c.getCelestaType());
    Expr expr = mv.getAggregateColumns().get(c.getName());
    assertTrue(expr instanceof Sum);

    c = mv.getColumn("f3");
    assertTrue(c instanceof StringColumn);
    StringColumn stringColumn = (StringColumn) c;
    assertEquals(2, stringColumn.getLength());
  }


    @Test
    public void testParsingFailsWhenRefTableInAnotherGrain() {

        try {
            File f = ResourceUtil.getResourceAsFile(
                    ParserTest.class,
                    "materializedView/testParsingNotFailsWhenMaterializedViewSyntaxIsCorrect.sql"
            );
            //Парсим рабочую гранулу
            parse(f);
        } catch (Exception e) {
            //Этот участок рабочий и проверяется в другом тесте.
            throw new RuntimeException(e);
        }

        //Пытаемся распарсить гранулу, в которой MaterializedView ссылается на таблицу из первой
        File f = ResourceUtil.getResourceAsFile(
                ParserTest.class,
                "materializedView/testParsingFailsWhenRefTableInAnotherGrain.sql"
        );
        assertThrows(ParseException.class, () -> parse(f));
    }

  @Test
  public void testParsingFailsWithDateInAggregate() {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "materializedView/testParsingFailsWithDateInAggregate.sql"
    );
    assertThrows(ParseException.class, () -> parse(f));
  }

}

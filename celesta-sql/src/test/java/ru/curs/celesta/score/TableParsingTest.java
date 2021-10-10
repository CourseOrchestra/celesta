package ru.curs.celesta.score;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Created by ioann on 11.09.2017.
 */
public class TableParsingTest extends AbstractParsingTest {

  @Test
  public void testParsingFailsWhenSinglePkAndIndexMatches() {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "table/testParsingFailsWhenSinglePkAndIndexMatches.sql"
    );
    assertThrows(ParseException.class, () -> parse(f));
  }

  @Test
  public void testParsingFailsWhenComplexPkAndIndexMatches() {
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
    ReadOnlyTable rot = g.getTable("t1", ReadOnlyTable.class);
    assertTrue(rot.isAutoUpdate());

    Table t = g.getTable("t2", Table.class);
    assertTrue(t.isAutoUpdate());
    assertFalse(t.isVersioned());

    t = g.getTable("t3", Table.class);
    assertFalse(t.isAutoUpdate());
    assertTrue(t.isVersioned());

    t = g.getTable("t4", Table.class);
    assertFalse(t.isAutoUpdate());
    assertFalse(t.isVersioned());

    t = g.getTable("t5", Table.class);
    assertTrue(t.isAutoUpdate());
    assertTrue(t.isVersioned());
  }

  @Test
  void testDecimalParsingFailsWithZeroPrecision() {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "table/testDecimalParsingFailsWithZeroPrecision.sql"
    );
    assertThrows(ParseException.class, () -> parse(f));
  }

  @Test
  void testDecimalParsingFailsWithTooBigPrecision() {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "table/testDecimalParsingFailsWithTooBigPrecision.sql"
    );
    assertThrows(ParseException.class, () -> parse(f));
  }

  @Test
  void testDecimalParsingFailsWithNegativeScale() {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "table/testDecimalParsingFailsWithNegativeScale.sql"
    );
    assertThrows(ParseException.class, () -> parse(f));
  }

  @Test
  void testDecimalParsingFailsWhenScaleMoreThanPrecision() {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "table/testDecimalParsingFailsWhenScaleMoreThanPrecision.sql"
    );
    assertThrows(ParseException.class, () -> parse(f));
  }

  @Test
  void testDecimalParsingFailsWhenDefaultIsNotCorrect() {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "table/testDecimalParsingFailsWhenDefaultIsNotCorrect.sql"
    );
    assertThrows(ParseException.class, () -> parse(f));
  }

  @Test
  void testDecimal() throws Exception {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "table/testDecimal.sql"
    );
    Grain g = parse(f);
    BasicTable t1 = g.getElement("t1", BasicTable.class);
    DecimalColumn dc1 = (DecimalColumn)t1.getColumn("cost");
    BasicTable t2 = g.getElement("t2", BasicTable.class);
    DecimalColumn dc2 = (DecimalColumn)t2.getColumn("cost");
    BasicTable t3 = g.getElement("t3", BasicTable.class);
    DecimalColumn dc3 = (DecimalColumn)t3.getColumn("cost");
    BasicTable t4 = g.getElement("t4", BasicTable.class);
    DecimalColumn dc4 = (DecimalColumn)t4.getColumn("cost");

    assertAll(
            // cost decimal(1, 0) default 4.0
            () -> assertEquals("cost", dc1.getName()),
            () -> assertEquals(1, dc1.getPrecision()),
            () -> assertEquals(0, dc1.getScale()),
            () -> assertEquals(new BigDecimal("4.0"), dc1.getDefaultValue()),
            () -> assertTrue(dc1.isNullable()),
            //cost decimal(38, 0)
            () -> assertEquals("cost", dc2.getName()),
            () -> assertEquals(38, dc2.getPrecision()),
            () -> assertEquals(0, dc2.getScale()),
            () -> assertNull(dc2.getDefaultValue()),
            () -> assertTrue(dc2.isNullable()),
            //cost decimal(38, 37) DEFAULT 0.1234
            () -> assertEquals("cost", dc3.getName()),
            () -> assertEquals(38, dc3.getPrecision()),
            () -> assertEquals(37, dc3.getScale()),
            () -> assertEquals(new BigDecimal("0.1234"), dc3.getDefaultValue()),
            () -> assertTrue(dc3.isNullable()),
            //cost decimal(5, 4) not null DEFAULT 1.134,
            () -> assertEquals("cost", dc4.getName()),
            () -> assertEquals(5, dc4.getPrecision()),
            () -> assertEquals(4, dc4.getScale()),
            () -> assertEquals(new BigDecimal("1.134"), dc4.getDefaultValue()),
            () -> assertFalse(dc4.isNullable())
    );
  }

  @Test
  void testParsingFailsWhenDatetimeWithTimeZoneHasDefaultValue() {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "table/testParsingFailsWhenDatetimeWithTimeZoneHasDefaultValue.sql"
    );
    assertThrows(ParseException.class, () -> parse(f));
  }

  @Test
  void testDatetimeWithTimeZone() throws Exception {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "table/testDatetimeWithTimeZone.sql"
    );
    Grain g = parse(f);
    BasicTable t1 = g.getElement("t", BasicTable.class);
    ZonedDateTimeColumn c = (ZonedDateTimeColumn)t1.getColumn("created");

    assertAll(
            // created datetime with time zone default null
            () -> assertEquals("created", c.getName()),
            () -> assertNull(c.getDefaultValue()),
            () -> assertTrue(c.isNullable())
    );
  }

}

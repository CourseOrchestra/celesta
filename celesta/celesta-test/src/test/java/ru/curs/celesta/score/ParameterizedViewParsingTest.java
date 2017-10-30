package ru.curs.celesta.score;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

import java.util.Map;


/**
 * Created by ioann on 09.08.2017.
 */
public class ParameterizedViewParsingTest extends AbstractParsingTest {

  @Test
  public void testParsingNotFailsWhenParameterizedViewSyntaxIsCorrect() throws Exception {
    ChecksumInputStream input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream("parameterizedView/testParsingNotFailsWhenParameterizedViewSyntaxIsCorrect.sql"));
    CelestaParser cp = new CelestaParser(input);
    Grain g = cp.grain(s, "test");

    assertEquals(3, g.getElements(ParameterizedView.class).size());

    ParameterizedView pv = g.getElement("pView1", ParameterizedView.class);
    Map<String, Parameter> params = pv.getParameters();
    assertEquals(1, params.size());
    Parameter p = params.get("p");
    assertEquals("p", p.getName());
    assertEquals(ViewColumnType.INT, p.getType());

    pv = g.getElement("pView2", ParameterizedView.class);
    params = pv.getParameters();
    assertEquals(2, params.size());
    p = params.get("p1");
    assertEquals("p1", p.getName());
    assertEquals(ViewColumnType.INT, p.getType());
    p = params.get("p2");
    assertEquals("TEST", p.getCelestaDoc());
    assertEquals("p2", p.getName());
    assertEquals(ViewColumnType.TEXT, p.getType());


    pv = g.getElement("pView3", ParameterizedView.class);
    params = pv.getParameters();
    assertEquals(1, params.size());
    p = params.get("p");
    assertEquals("p", p.getName());
    assertEquals(ViewColumnType.INT, p.getType());
  }

  @Test
  public void testParsingFailsWhenParamIsNotUsed() throws ParseException {
    ChecksumInputStream input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream("parameterizedView/testParsingFailsWhenParamIsNotUsed.sql"));
    CelestaParser cp = new CelestaParser(input);
    assertThrows(ParseException.class, () -> cp.grain(s, "test"));
  }

  @Test
  public void testParsingFailsWithoutParams() throws ParseException {
    ChecksumInputStream input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream("parameterizedView/testParsingFailsWithoutParams.sql"));
    CelestaParser cp = new CelestaParser(input);
    assertThrows(ParseException.class, () -> cp.grain(s, "test"));
  }

  @Test
  public void testParsingFailsWhenUndeclaredParamIsUsed() throws ParseException {
    ChecksumInputStream input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream("parameterizedView/testParsingFailsWhenUndeclaredParamIsUsed.sql"));
    CelestaParser cp = new CelestaParser(input);
    assertThrows(ParseException.class, () -> cp.grain(s, "test"));
  }

  @Test
  public void testParsingFailsWhenParamDeclarationIsDuplicated() throws ParseException {
    ChecksumInputStream input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream("parameterizedView/testParsingFailsWhenParamDeclarationIsDuplicated.sql"));
    CelestaParser cp = new CelestaParser(input);
    assertThrows(ParseException.class, () -> cp.grain(s, "test"));
  }
}



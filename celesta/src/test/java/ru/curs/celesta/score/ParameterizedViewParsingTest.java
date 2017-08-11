package ru.curs.celesta.score;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

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
    assertEquals("p2", p.getName());
    assertEquals(ViewColumnType.TEXT, p.getType());


    pv = g.getElement("pView3", ParameterizedView.class);
    params = pv.getParameters();
    assertEquals(1, params.size());
    p = params.get("p");
    assertEquals("p", p.getName());
    assertEquals(ViewColumnType.INT, p.getType());
  }

  @Test(expected = ParseException.class)
  public void testParsingFailsWhenParamIsNotUsed() throws ParseException {
    ChecksumInputStream input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream("parameterizedView/testParsingFailsWhenParamIsNotUsed.sql"));
    CelestaParser cp = new CelestaParser(input);
    cp.grain(s, "test");
  }

  @Test(expected = ParseException.class)
  public void testParsingFailsWithoutParams() throws ParseException {
    ChecksumInputStream input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream("parameterizedView/testParsingFailsWithoutParams.sql"));
    CelestaParser cp = new CelestaParser(input);
    cp.grain(s, "test");
  }

  @Test(expected = ParseException.class)
  public void testParsingFailsWhenUndeclaredParamIsUsed() throws ParseException {
    ChecksumInputStream input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream("parameterizedView/testParsingFailsWhenUndeclaredParamIsUsed.sql"));
    CelestaParser cp = new CelestaParser(input);
    cp.grain(s, "test");
  }

  @Test(expected = ParseException.class)
  public void testParsingFailsWhenParamDeclarationIsDuplicated() throws ParseException {
    ChecksumInputStream input = new ChecksumInputStream(
        ParserTest.class.getResourceAsStream("parameterizedView/testParsingFailsWhenParamDeclarationIsDuplicated.sql"));
    CelestaParser cp = new CelestaParser(input);
    cp.grain(s, "test");
  }
}



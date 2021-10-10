package ru.curs.celesta.score;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;


/**
 * Created by ioann on 09.08.2017.
 */
public class ParameterizedViewParsingTest extends AbstractParsingTest {

  @Test
  public void testParsingNotFailsWhenParameterizedViewSyntaxIsCorrect() throws Exception {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "parameterizedView/testParsingNotFailsWhenParameterizedViewSyntaxIsCorrect.sql"
    );
    Grain g = parse(f);

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
  public void testParsingFailsWhenParamIsNotUsed() {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "parameterizedView/testParsingFailsWhenParamIsNotUsed.sql"
    );
    assertThrows(ParseException.class, () -> parse(f));
  }

  @Test
  public void testParsingFailsWithoutParams() {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "parameterizedView/testParsingFailsWithoutParams.sql"
    );
    assertThrows(ParseException.class, () -> parse(f));
  }

  @Test
  public void testParsingFailsWhenUndeclaredParamIsUsed() {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "parameterizedView/testParsingFailsWhenUndeclaredParamIsUsed.sql"
    );
    assertThrows(ParseException.class, () -> parse(f));
  }

  @Test
  public void testParsingFailsWhenParamDeclarationIsDuplicated() {
    File f = ResourceUtil.getResourceAsFile(
            ParserTest.class,
            "parameterizedView/testParsingFailsWhenParamDeclarationIsDuplicated.sql"
    );
    assertThrows(ParseException.class, () -> parse(f));
  }
}



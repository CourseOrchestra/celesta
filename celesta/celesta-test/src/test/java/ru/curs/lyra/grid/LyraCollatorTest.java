package ru.curs.lyra.grid;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

import ru.curs.celesta.CelestaException;

public class LyraCollatorTest {

	@Test
	public void test0() {
		String rule1 = "";
		CollatorRulesLexer lexer = new CollatorRulesLexer(rule1);
		assertEquals(CollatorRulesLexer.END_OF_RULES, lexer.next());

		rule1 = " ";
		lexer = new CollatorRulesLexer(rule1);
		assertEquals(CollatorRulesLexer.END_OF_RULES, lexer.next());

		rule1 = "  <    '<'  ";
		lexer = new CollatorRulesLexer(rule1);
		assertEquals(CollatorRulesLexer.PRIMARY_SEPARATOR, lexer.next());
		assertEquals(CollatorRulesLexer.CHARACTER, lexer.next());
		assertEquals('<', lexer.getValue());
		assertEquals(CollatorRulesLexer.END_OF_RULES, lexer.next());

	}

	@Test
	public void test1() {
		String rule1 = "bs Ыe' '<; 's'b,''''<'";
		CollatorRulesLexer lexer = new CollatorRulesLexer(rule1);
		assertEquals(CollatorRulesLexer.CHARACTER, lexer.next());
		assertEquals('b', lexer.getValue());
		assertEquals(CollatorRulesLexer.CHARACTER, lexer.next());
		assertEquals('s', lexer.getValue());
		assertEquals(CollatorRulesLexer.CHARACTER, lexer.next());
		assertEquals('Ы', lexer.getValue());
		assertEquals(CollatorRulesLexer.CHARACTER, lexer.next());
		assertEquals('e', lexer.getValue());
		assertEquals(CollatorRulesLexer.CHARACTER, lexer.next());
		assertEquals(' ', lexer.getValue());
		assertEquals(CollatorRulesLexer.PRIMARY_SEPARATOR, lexer.next());
		assertEquals(CollatorRulesLexer.SECONDARY_SEPARATOR, lexer.next());
		assertEquals(CollatorRulesLexer.CHARACTER, lexer.next());
		assertEquals('s', lexer.getValue());
		assertEquals(CollatorRulesLexer.CHARACTER, lexer.next());
		assertEquals('b', lexer.getValue());
		assertEquals(CollatorRulesLexer.TERNARY_SEPARATOR, lexer.next());

		assertEquals(CollatorRulesLexer.CHARACTER, lexer.next());
		assertEquals('\'', lexer.getValue());
		assertEquals(CollatorRulesLexer.CHARACTER, lexer.next());
		assertEquals('<', lexer.getValue());
		assertEquals(CollatorRulesLexer.END_OF_RULES, lexer.next());

	}

	@Test
	public void test2() {
		String rule1 = "a  < ''bcd' < c";
		CollatorRulesLexer lexer = new CollatorRulesLexer(rule1);
		assertEquals(CollatorRulesLexer.CHARACTER, lexer.next());
		assertEquals('a', lexer.getValue());
		assertEquals(CollatorRulesLexer.PRIMARY_SEPARATOR, lexer.next());
		assertEquals(CollatorRulesLexer.CHARACTER, lexer.next());
		assertEquals('d', lexer.getValue());
		assertEquals(CollatorRulesLexer.PRIMARY_SEPARATOR, lexer.next());
		assertEquals(CollatorRulesLexer.CHARACTER, lexer.next());
		assertEquals('c', lexer.getValue());
	}

	@Test
	public void test3() throws CelestaException, LyraCollatorException {
		LyraCollator lc = LyraCollator.getInstance("mo<a < b,c < d,g;e,k;f,w<'~'", "TEST");
		assertEquals(4, lc.getPrimOrderCount());
		assertEquals(3, lc.getSecOrderCount());
		assertEquals(2, lc.getTerOrderCount());

		LyraCollationElementIterator i = lc.getCollationElementIterator("cmaeow~");
		assertTrue(i.next());
		assertEquals(1, i.primaryOrder());
		assertEquals(0, i.secondaryOrder());
		assertEquals(1, i.tertiaryOrder());

		assertTrue(i.next());
		assertEquals(0, i.primaryOrder());
		assertEquals(0, i.secondaryOrder());
		assertEquals(0, i.tertiaryOrder());

		assertTrue(i.next());
		assertEquals(2, i.primaryOrder());
		assertEquals(1, i.secondaryOrder());
		assertEquals(0, i.tertiaryOrder());

		assertTrue(i.next());
		assertEquals(2, i.primaryOrder());
		assertEquals(2, i.secondaryOrder());
		assertEquals(1, i.tertiaryOrder());

		assertTrue(i.next());
		assertEquals(3, i.primaryOrder());
		assertEquals(0, i.secondaryOrder());
		assertEquals(0, i.tertiaryOrder());

		assertFalse(i.next());

		i = lc.getCollationElementIterator("ooo");
		assertFalse(i.next());
		i = lc.getCollationElementIterator("");
		assertFalse(i.next());

	}

	@Test
	public void test4() {
		LyraCollator lc1 = LyraCollator.getInstance("<a<b<c", "TEST1");
		LyraCollator lc2 = LyraCollator.getInstance("<d<e<f", "TEST2");
		LyraCollator lc3 = LyraCollator.getInstance("<a<b<c", "TEST3");
		LyraCollator lc4 = LyraCollator.getInstance("<d<e<f", "TEST4");
		assertEquals(lc1, lc3);
		assertEquals("TEST1", lc1.getName());
		assertEquals("TEST1", lc3.getName());
		
		assertEquals(lc2, lc4);
		assertEquals("TEST2", lc2.getName());
		assertEquals("TEST2", lc4.getName());
		
		assertTrue(lc1 != lc2);

		assertEquals(3, lc1.getPrimOrderCount());
		assertEquals(1, lc1.getSecOrderCount());
		assertEquals(1, lc1.getTerOrderCount());
	}
}

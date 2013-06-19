package ru.curs.celesta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import org.junit.Test;

public class ScoreTest {
	@Test
	public void test1() throws CelestaException, ParseException {
		Score s = new Score("score;test");
		Grain g1 = s.getGrain("g1");
		Grain g2 = s.getGrain("g2");
		assertEquals("g2", g2.getName());
		Table b = g2.getTables().get("b");
		assertEquals(1, b.getForeignKeys().size());
		Table a = b.getForeignKeys().iterator().next().getReferencedTable();
		assertEquals("a", a.getName());
		assertSame(g1, a.getGrain());
	}
}

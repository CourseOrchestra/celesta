package ru.curs.celesta.score;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import org.junit.Test;

import ru.curs.celesta.score.CelestaException;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.Table;

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

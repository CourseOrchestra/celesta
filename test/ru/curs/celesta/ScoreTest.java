package ru.curs.celesta;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ScoreTest {
	@Test
	public void test1() throws CelestaException, ParseException {
		Score s = new Score("score;test");
		assertEquals("g2", s.getGrain("g2").getName());
	}
}

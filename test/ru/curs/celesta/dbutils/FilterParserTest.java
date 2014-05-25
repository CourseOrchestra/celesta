package ru.curs.celesta.dbutils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.filter.FilterParser;
import ru.curs.celesta.dbutils.filter.FilterParser.FilterType;

public class FilterParserTest {

	@Test
	public void test1() throws CelestaException {
		String result;

		result = FilterParser.translateFilter(FilterType.NUMERIC, "foo",
				"(5 |> 6 |!3)&!null");
		assertEquals(
				"(foo = 5 or foo > 6 or not (foo = 3)) and not (foo is null)",
				result);

		result = FilterParser.translateFilter(FilterType.NUMERIC, "foo",
				"(5|11..15|6..|..5)&!null");
		assertEquals(
				"(foo = 5 or foo between 11 and 15 or foo >= 6 or foo <= 5) and not (foo is null)",
				result);

		result = FilterParser.translateFilter(FilterType.TEXT, "foo",
				"('aaa'&'bb')|(!'ddd'&!null)");
		assertEquals(
				"(foo = 'aaa' and foo = 'bb') or (not (foo = 'ddd') and not (foo is null))",
				result);

		result = FilterParser.translateFilter(FilterType.TEXT, "foo",
				"'5'|..'11'|'6'..|'a'..'b'|%'5'|'abc'%|!%'ef'%|null");
		assertEquals(
				"foo = '5' or foo <= '11' or foo >= '6' or foo between 'a' and 'b' or foo like '%5' "
						+ "or foo like 'abc%' or not (foo like '%ef%') or foo is null",
				result);

		result = FilterParser.translateFilter(FilterType.TEXT, "foo",
				"@'q'|@..'cC'|@'Ff'..|@'a'..'b'|@%'5a'|'abc'%|! @ %'ef'%|null");
		assertEquals(
				"UPPER(foo) = 'Q' or UPPER(foo) <= 'CC' or UPPER(foo) >= 'FF' or UPPER(foo) between 'A' and 'B' or UPPER(foo) like '%5A' "
						+ "or foo like 'abc%' or not (UPPER(foo) like '%EF%') or foo is null",
				result);

		result = FilterParser.translateFilter(FilterType.OTHER, "foo", "!NULL");
		assertEquals("not (foo is null)", result);
	}

	@Test
	public void test2() throws CelestaException {
		boolean itWas = false;
		try {
			FilterParser.translateFilter(FilterType.NUMERIC, "foo",
					"(5 ||> 6 |!3)&!null");
		} catch (CelestaException e) {
			itWas = true;
		}
		assertTrue(itWas);
	}

	@Test
	public void test3() throws CelestaException {
		String result;
		result = FilterParser.translateFilter(FilterType.TEXT, "foo",
				"'abc'%'ef'%'g''h'");
		assertEquals("foo like 'abc%ef%g''h'", result);
		result = FilterParser.translateFilter(FilterType.TEXT, "foo",
				"%'.'%'.'%");
		assertEquals("foo like '%.%.%'", result);
		result = FilterParser.translateFilter(FilterType.TEXT, "foo",
				"%'asdf'%'g''h'%");
		assertEquals("foo like '%asdf%g''h%'", result);
	}
}

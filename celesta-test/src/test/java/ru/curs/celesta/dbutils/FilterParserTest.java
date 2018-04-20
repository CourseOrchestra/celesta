package ru.curs.celesta.dbutils;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.filter.FilterParser;
import ru.curs.celesta.dbutils.filter.FilterParser.FilterType;
import ru.curs.celesta.score.DateTimeColumn;
import ru.curs.celesta.score.ParseException;

public class FilterParserTest {

	private FilterParser.SQLTranslator tr = new FilterParser.SQLTranslator() {
		@Override
		public String translateDate(String date) {
			try {
				DateTimeColumn.parseISODate(date);
				return "D" + date;
			} catch (ParseException e) {
				throw new CelestaException(e.getMessage());
			}

		}

	};

	@Test
	public void test1() {
		String result;

		result = FilterParser.translateFilter(FilterType.NUMERIC, "foo",
				"(5 |> 6 |!3)&!null", tr);
		assertEquals(
				"(foo = 5 or foo > 6 or not (foo = 3)) and not (foo is null)",
				result);

		result = FilterParser.translateFilter(FilterType.NUMERIC, "foo",
				"(5|11..15|6..|..5)&!null", tr);
		assertEquals(
				"(foo = 5 or foo between 11 and 15 or foo >= 6 or foo <= 5) and not (foo is null)",
				result);

		result = FilterParser.translateFilter(FilterType.TEXT, "foo",
				"('aaa'&'bb')|(!'ddd'&!null)", tr);
		assertEquals(
				"(foo = 'aaa' and foo = 'bb') or (not (foo = 'ddd') and not (foo is null))",
				result);

		result = FilterParser.translateFilter(FilterType.TEXT, "foo",
				"'5'|..'11'|'6'..|'a'..'b'|%'5'|'abc'%|!%'ef'%|null", tr);
		assertEquals(
				"foo = '5' or foo <= '11' or foo >= '6' or foo between 'a' and 'b' or foo like '%5' "
						+ "or foo like 'abc%' or not (foo like '%ef%') or foo is null",
				result);

		result = FilterParser.translateFilter(FilterType.TEXT, "foo",
				"@'q'|@..'cC'|@'Ff'..|@'a'..'b'|@%'5a'|'abc'%|! @ %'ef'%|null",
				tr);
		assertEquals(
				"UPPER(foo) = 'Q' or UPPER(foo) <= 'CC' or UPPER(foo) >= 'FF' or UPPER(foo) between 'A' and 'B' or UPPER(foo) like '%5A' "
						+ "or foo like 'abc%' or not (UPPER(foo) like '%EF%') or foo is null",
				result);

		result = FilterParser.translateFilter(FilterType.OTHER, "foo", "!NULL",
				tr);
		assertEquals("not (foo is null)", result);
	}

	@Test
	public void test15() {
		String result;
		result = FilterParser.translateFilter(FilterType.NUMERIC, "baz",
				"-5 .. 10&!-3.5", tr);
		assertEquals("baz between -5 and 10 and not (baz = -3.5)", result);
	}

	@Test
	public void test2() {
		boolean itWas = false;
		try {
			FilterParser.translateFilter(FilterType.NUMERIC, "foo",
					"(5 ||> 6 |!3)&!null", tr);
		} catch (CelestaException e) {
			itWas = true;
		}
		assertTrue(itWas);
	}

	@Test
	public void test3() {
		String result;
		result = FilterParser.translateFilter(FilterType.TEXT, "foo",
				"'abc'%'ef'%'g''h'", tr);
		assertEquals("foo like 'abc%ef%g''h'", result);
		result = FilterParser.translateFilter(FilterType.TEXT, "foo",
				"%'.'%'.'%", tr);
		assertEquals("foo like '%.%.%'", result);
		result = FilterParser.translateFilter(FilterType.TEXT, "foo",
				"%'asdf'%'g''h'%", tr);
		assertEquals("foo like '%asdf%g''h%'", result);
	}

	@Test
	public void test4() {

		String result;
		result = FilterParser.translateFilter(FilterType.DATETIME, "bar",
				"'20131124'", tr);
		assertEquals("bar = D'20131124'", result);

		result = FilterParser.translateFilter(FilterType.DATETIME, "bar",
				"'20131124'..'20151211'|'20111111'", tr);
		assertEquals(
				"bar between D'20131124' and D'20151211' or bar = D'20111111'",
				result);

		result = FilterParser.translateFilter(FilterType.DATETIME, "bar",
				"(>'20131124'&..'20151211')|'20111111'..", tr);
		assertEquals(
				"(bar > D'20131124' and bar <= D'20151211') or bar >= D'20111111'",
				result);

		boolean itWas = false;
		try {
			FilterParser.translateFilter(FilterType.DATETIME, "foo",
					"'20132324'", tr);
		} catch (CelestaException e) {
			// e.printStackTrace();
			itWas = true;
		}
		assertTrue(itWas);
		try {
			FilterParser.translateFilter(FilterType.DATETIME, "foo",
					"'20131144'", tr);
		} catch (CelestaException e) {
			itWas = true;
		}
		assertTrue(itWas);
	}

}

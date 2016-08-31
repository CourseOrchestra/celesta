package ru.curs.celesta.dbutils;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.ColumnMeta;
import ru.curs.celesta.score.Expr;
import ru.curs.celesta.score.SQLGenerator;
import ru.curs.celesta.score.StringColumn;

public class NavigationQueriesMakerTest {

	class Params implements WhereMakerParamsProvider {

		boolean init = false;
		boolean nf = true;
		String[] fields;
		int[] sfIndices;
		boolean[] descOrders;
		Object[] values;
		Map<String, Boolean> nullables = new HashMap<>();
		Map<String, AbstractFilter> filters = Collections.emptyMap();

		Params(String[] orderByNames, boolean[] descOrders, boolean[] nullables, boolean[] nullsMask) {
			this.fields = orderByNames;
			sfIndices = new int[fields.length];
			values = new Object[fields.length];
			for (int i = 0; i < fields.length; i++) {
				this.nullables.put(fields[i], nullables[i]);

				fields[i] = '"' + fields[i] + '"';
				sfIndices[i] = i;
				values[i] = nullsMask[i] ? null : Integer.valueOf(1);
			}
			this.descOrders = descOrders;
		}

		Params(String[] orderByNames, boolean[] descOrders, boolean[] nullables) {
			this(orderByNames, descOrders, nullables, new boolean[nullables.length]);
		}

		void setNullsFirst(boolean nullsFirst) {
			this.nf = nullsFirst;
		}

		@Override
		public String[] sortFields() {
			return fields;
		}

		@Override
		public boolean[] descOrders() {
			return descOrders;
		}

		@Override
		public boolean isNullable(String columnName) {
			return nullables.get(columnName);
		}

		@Override
		public Map<String, AbstractFilter> filters() {
			return filters;
		}

		protected void setFilters(Map<String, AbstractFilter> filters) {
			this.filters = filters;
		}

		@Override
		public QueryBuildingHelper dba() {
			return new QueryBuildingHelper() {

				@Override
				public String translateDate(String date) throws CelestaException {
					return date;
				}

				@Override
				public boolean nullsFirst() {
					return nf;
				}

				@Override
				public SQLGenerator getViewSQLGenerator() {
					return null;
				}

			};
		}

		@Override
		public void initOrderBy() throws CelestaException {
			init = true;
		}

		@Override
		public Expr complexFilter() {
			return null;
		}

		@Override
		public int[] sortFieldsIndices() throws CelestaException {
			return sfIndices;
		}

		@Override
		public Object[] values() throws CelestaException {
			return values;
		}
	}

	private static boolean[] a(boolean... args) {
		return args;
	}

	private static String[] a(String... args) {
		return args;
	}

	@Test
	public void test1() throws CelestaException {

		Params p = new Params(a("grainid", "tablename"), a(false, false), a(false, false));
		WhereTermsMaker c = new WhereTermsMaker(p);

		assertFalse(p.init);

		assertEquals("((\"grainid\" <= ?) and ((\"grainid\" < ?) or (\"tablename\" < ?)))", c.getWhereTerm('<').getWhere());

		assertTrue(p.init);

		assertEquals("((\"grainid\" >= ?) and ((\"grainid\" > ?) or (\"tablename\" > ?)))", c.getWhereTerm('>').getWhere());
		assertEquals("((\"grainid\" = ?) and (\"tablename\" = ?))", c.getWhereTerm('=').getWhere());
	}

	@Test
	public void test2() throws CelestaException {
		Params p = new Params(a("d", "i", "m", "grainid", "tablename"), a(false, false, true, false, false),
				a(false, false, false, false, false, false));
		WhereTermsMaker c = new WhereTermsMaker(p);

		assertEquals(
				"((\"d\" >= ?) and ((\"d\" > ?) or ((\"i\" >= ?) and ((\"i\" > ?) or ((\"m\" <= ?) and ((\"m\" < ?) or ((\"grainid\" >= ?) and ((\"grainid\" > ?) or (\"tablename\" > ?)))))))))",
				c.getWhereTerm('>').getWhere());
		assertEquals(
				"((\"d\" <= ?) and ((\"d\" < ?) or ((\"i\" <= ?) and ((\"i\" < ?) or ((\"m\" >= ?) and ((\"m\" > ?) or ((\"grainid\" <= ?) and ((\"grainid\" < ?) or (\"tablename\" < ?)))))))))",
				c.getWhereTerm('<').getWhere());

	}

	@Test
	public void test3() throws CelestaException {
		Params p = new Params(a("grainid", "m", "tablename"), a(false, true, false), a(false, false, false));
		WhereTermsMaker c = new WhereTermsMaker(p);

		assertEquals(
				"((\"grainid\" >= ?) and ((\"grainid\" > ?) or ((\"m\" <= ?) and ((\"m\" < ?) or (\"tablename\" > ?)))))",
				c.getWhereTerm('>').getWhere());
		assertEquals(
				"((\"grainid\" <= ?) and ((\"grainid\" < ?) or ((\"m\" >= ?) and ((\"m\" > ?) or (\"tablename\" < ?)))))",
				c.getWhereTerm('<').getWhere());

		p = new Params(a("grainid", "m", "tablename"), a(false, true, false), a(true, true, true));
		c = new WhereTermsMaker(p);

		assertEquals(
				"((\"grainid\" >= ?) and ((\"grainid\" > ?) or (((\"m\" <= ?) or (\"m\" is null)) and ((\"m\" < ?) or (\"m\" is null) or (\"tablename\" > ?)))))",
				c.getWhereTerm('>').getWhere());

		p.setNullsFirst(false);
		assertEquals(
				"(((\"grainid\" >= ?) or (\"grainid\" is null)) and ((\"grainid\" > ?) or (\"grainid\" is null) or ((\"m\" <= ?) and ((\"m\" < ?) or (\"tablename\" > ?) or (\"tablename\" is null)))))",
				c.getWhereTerm('>').getWhere());

	}

	@Test
	public void test4() throws CelestaException {
		Params p = new Params(a("A"), a(false), a(false));
		WhereTermsMaker c = new WhereTermsMaker(p);
		assertEquals("(\"A\" > ?)", c.getWhereTerm('>').getWhere());
		assertEquals("(\"A\" = ?)", c.getWhereTerm('=').getWhere());

		p = new Params(a("A"), a(false), a(true), a(true));
		c = new WhereTermsMaker(p);
		assertEquals("(not (\"A\" is null))", c.getWhereTerm('>').getWhere());
		assertEquals("(\"A\" is null)", c.getWhereTerm('=').getWhere());

		p = new Params(a("A"), a(false), a(false), a(true));
		c = new WhereTermsMaker(p);
		// will always be true, A is not nullable
		assertEquals("(1 = 1)", c.getWhereTerm('>').getWhere());
		assertEquals("(1 = 0)", c.getWhereTerm('=').getWhere());
		p.setNullsFirst(false);
		// will always be false, we are at the end
		assertEquals("(1 = 0)", c.getWhereTerm('>').getWhere());
		assertEquals("(1 = 0)", c.getWhereTerm('=').getWhere());

		p = new Params(a("A"), a(false), a(true));
		c = new WhereTermsMaker(p);
		assertEquals("(\"A\" > ?)", c.getWhereTerm('>').getWhere());
		assertEquals("(\"A\" = ?)", c.getWhereTerm('=').getWhere());
		p.setNullsFirst(false);
		assertEquals("((\"A\" > ?) or (\"A\" is null))", c.getWhereTerm('>').getWhere());
		assertEquals("(\"A\" = ?)", c.getWhereTerm('=').getWhere());

		assertEquals("(\"A\" < ?)", c.getWhereTerm('<').getWhere());
		p.setNullsFirst(true);
		assertEquals("((\"A\" < ?) or (\"A\" is null))", c.getWhereTerm('<').getWhere());

		p = new Params(a("A"), a(false), a(true), a(true));
		c = new WhereTermsMaker(p);
		assertEquals("(not (\"A\" is null))", c.getWhereTerm('>').getWhere());
		p.setNullsFirst(false);
		assertEquals("(1 = 0)", c.getWhereTerm('>').getWhere());

		assertEquals("(not (\"A\" is null))", c.getWhereTerm('<').getWhere());

	}

	@Test
	public void test5() throws CelestaException {
		Params p = new Params(a("A", "B"), a(false, false), a(true, true));
		WhereTermsMaker c = new WhereTermsMaker(p);

		p.setNullsFirst(false);
		assertEquals(
				"(((\"A\" >= ?) or (\"A\" is null)) and ((\"A\" > ?) or (\"A\" is null) or (\"B\" > ?) or (\"B\" is null)))",
				c.getWhereTerm('>').getWhere());
		assertEquals("((\"A\" <= ?) and ((\"A\" < ?) or (\"B\" < ?)))", c.getWhereTerm('<').getWhere());
		assertEquals("((\"A\" = ?) and (\"B\" = ?))", c.getWhereTerm('=').getWhere());

		p.setNullsFirst(true);
		assertEquals(
				"(((\"A\" <= ?) or (\"A\" is null)) and ((\"A\" < ?) or (\"A\" is null) or (\"B\" < ?) or (\"B\" is null)))",
				c.getWhereTerm('<').getWhere());
		assertEquals("((\"A\" = ?) and (\"B\" = ?))", c.getWhereTerm('=').getWhere());

		p = new Params(a("A", "B"), a(false, false), a(true, true), a(true, false));
		c = new WhereTermsMaker(p);
		assertEquals("((not (\"A\" is null)) or (\"B\" > ?))", c.getWhereTerm('>').getWhere());
		assertEquals("((\"A\" is null) and (\"B\" = ?))", c.getWhereTerm('=').getWhere());
	}

	@Test
	public void test6() throws CelestaException {
		Params p = new Params(a("A"), a(false), a(true));
		p.setNullsFirst(false);
		WhereTermsMaker c = new WhereTermsMaker(p);
		assertEquals("((\"A\" > ?) or (\"A\" is null))", c.getWhereTerm('>').getWhere());

		Map<String, AbstractFilter> filters = new HashMap<>();
		filters.put("A", new SingleValue(4));
		p.setFilters(filters);
		// Range filter must makes the system treat field as not nullable!
		assertEquals("((\"A\" = ?) and (\"A\" > ?))", c.getWhereTerm('>').getWhere());
		filters.put("A", new Range(4, 5));
		assertEquals("((\"A\" between ? and ?) and (\"A\" > ?))", c.getWhereTerm('>').getWhere());

		filters.put("A", new Filter("null|'foo'", new ColumnMeta() {

			@Override
			public String jdbcGetterName() {
				return null;
			}

			@Override
			public String getCelestaType() {
				return StringColumn.VARCHAR;
			}

			@Override
			public boolean isNullable() {
				return true;
			}

			@Override
			public String getCelestaDoc() {
				return "";
			}
		}));

		filters.put("B", new SingleValue(1));

		assertEquals("((\"B\" = ?) and (\"A\" is null or \"A\" = 'foo') and ((\"A\" > ?) or (\"A\" is null)))",
				c.getWhereTerm('>').getWhere());

	}
}

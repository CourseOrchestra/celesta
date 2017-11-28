package ru.curs.lyra.grid;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.adaptors.StaticDataAdaptor;

public class VarcharFieldEnumeratorTest {

	private static final double EPSILON = 1e-10;

	private static final String RULES = "<а<б<в<г<д<е<ж<з<и<й<к<л<м<н<о<п<р<с<т<у<ф<х<ц<ч<ш<щ<ъ<ы<ь<э<ю<я";

	private static final StaticDataAdaptor CUSTOM_DBA = new StaticDataAdaptor() {
		@Override
		public List<String> selectStaticStrings(List<String> data, String columnName, String orderType) throws CelestaException {
			return Arrays.asList(
					"а","б","в","г","д","е","ж","з","и","й","к","л","м","н",
					"о","п","р","с","т","у","ф","х",
					"ц","ч","ш","щ","ъ","ы","ь","э","ю","я");
		}

		@Override
		public int compareStrings(String left, String right) throws CelestaException {
			if (left.equalsIgnoreCase(right))
				return left.compareToIgnoreCase(right);
			return left.compareTo(right);
		}
	};


	public static final StaticDataAdaptor DBA = new StaticDataAdaptor() {
		@Override
		public List<String> selectStaticStrings(List<String> data, String columnName, String orderType) throws CelestaException {
			List<String> result = new ArrayList<>(VarcharFieldEnumerator.CHARS);
			Collections.sort(result);
			return result;
		}

		@Override
		public int compareStrings(String left, String right) throws CelestaException {
			if (left.equalsIgnoreCase(right))
				return left.compareToIgnoreCase(right);
			return left.compareTo(right);
		}
	};

	private VarcharFieldEnumerator getManager(String min, int len) throws CelestaException {

		char[] max = new char[len];
		for (int i = 0; i < len; i++)
			max[i] = 'я';

		VarcharFieldEnumerator vfm = new VarcharFieldEnumerator(CUSTOM_DBA, min, new String(max), len);
		return vfm;
	}

	private VarcharFieldEnumerator getManager(int len) throws CelestaException {
		return getManager("", len);
	}

	@Test
	public void test1() throws CelestaException {
		VarcharFieldEnumerator km;
		km = new VarcharFieldEnumerator(CUSTOM_DBA, 1);
		km.setValue("");
		assertEquals(0.0, km.getPosition(), EPSILON);
		km.setValue("а");
		double a = km.getPosition();
		assertEquals(BigInteger.ONE, km.getOrderValue());
		km.setValue("б");
		double b = km.getPosition();
		assertEquals(a * 2, b, EPSILON);
		km.setValue("я");
		assertEquals(1.0, km.getPosition(), EPSILON);
		km.setValue("");
		assertEquals(0.0, km.getPosition(), EPSILON);

		km = getManager(4);
		km.setValue("");
		assertEquals(0.0, km.getPosition(), EPSILON);
		km.setValue("а");
		a = km.getPosition();
		km.setValue("ааа");
		b = km.getPosition();
		assertEquals(a * 3.0, b, EPSILON);
		km.setValue("яяяя");
		assertEquals(1.0, km.getPosition(), EPSILON);

		double v[] = new double[4];
		km.setValue("ван");
		v[0] = km.getPosition();
		km.setValue("маша");
		v[3] = km.getPosition();
		km.setValue("ваня");
		v[2] = km.getPosition();
		km.setValue("ваню");
		v[1] = km.getPosition();

		assertTrue(v[0] < v[1]);
		assertTrue(v[1] < v[2]);
		assertTrue(v[2] < v[3]);

		km = getManager(10);
		km.setValue("колюяяяяяя");
		a = km.getPosition();

		km.setValue("коля");
		b = km.getPosition();

		assertTrue(a < b);

		km.setValue("XXX");
		String msg = "";
		try {
			km.getPosition();
		} catch (CelestaException e) {
			msg = e.getMessage();
		}
		assertEquals("Error in string 'XXX': character 'X' is unknown for collator 'Collator'.", msg);

	}

	private void testReverse(VarcharFieldEnumerator km, String test) throws CelestaException {
		km.setValue(test);
		double p = km.getPosition();
		km.setValue("");
		km.setPosition(p);
		assertEquals(test, km.getValue());
	}

	@Test
	public void test2() throws CelestaException {
		VarcharFieldEnumerator km;
		km = getManager(4);
		testReverse(km, "а");
		testReverse(km, "бв");
		testReverse(km, "ваня");
		testReverse(km, "маша");
		testReverse(km, "яяяя");

		km = getManager(10);
		testReverse(km, "маша");
		testReverse(km, "коля");
		testReverse(km, "наташа");
	}

	@Test
	public void test3() throws CelestaException {
		VarcharFieldEnumerator km1, km2;
		km1 = getManager("ваня", 10);
		km2 = getManager("", 10);
		km2.setValue("яяяяяяяяяя");
		assertTrue(km1.cardinality().compareTo(km2.cardinality()) < 0);
		assertEquals(km2.getOrderValue().add(BigInteger.ONE), km2.cardinality());

		km1.setValue("ваня");
		assertEquals(0.0, km1.getPosition(), EPSILON);

		km2.setValue("наташа");
		BigInteger n = km2.getOrderValue();
		km2.setValue("ваня");
		n = n.subtract(km2.getOrderValue());
		km1.setValue("наташа");
		assertEquals(km1.getOrderValue(), n);

		km2.setValue("маша");
		n = km2.getOrderValue();
		km2.setValue("ваня");
		n = n.subtract(km2.getOrderValue());
		km1.setValue("маша");
		assertEquals(km1.getOrderValue(), n);

		km1 = getManager("коля", 10);
		km2.setValue("наташа");
		n = km2.getOrderValue();
		km2.setValue("коля");
		n = n.subtract(km2.getOrderValue());
		km1.setValue("наташа");
		assertEquals(km1.getOrderValue(), n);
	}

	@Test
	public void test4() throws CelestaException {
		VarcharFieldEnumerator km = new VarcharFieldEnumerator(CUSTOM_DBA, "ваня", "наташа", 7);
		km.setValue("коля");
		double k = km.getPosition();
		km.setValue("маша");
		double m = km.getPosition();
		km.setValue("ваня");
		assertEquals(0.0, km.getPosition(), EPSILON);
		assertTrue(0.0 < k && k < m && m < 1.0);
		km.setValue("наташа");
		assertEquals(1.0, km.getPosition(), EPSILON);

		km.setPosition(0.0);
		assertEquals("ваня", km.getValue());
		km.setPosition(k);
		assertEquals("коля", km.getValue());
		km.setPosition(m);
		assertEquals("маша", km.getValue());
		km.setPosition(1.0);
		assertEquals("наташа", km.getValue());

	}

	@Test
	public void test5() throws CelestaException {
		VarcharFieldEnumerator km = getManager(1);
		assertEquals("", km.getValue());
		assertEquals(BigInteger.ZERO, km.getOrderValue());
		km.setValue("а");
		assertEquals(BigInteger.ONE, km.getOrderValue());
		km.setValue("в");
		assertEquals(BigInteger.valueOf(3), km.getOrderValue());
		km.setValue("я");
		LyraCollator lc = LyraCollator.getInstance(RULES,CUSTOM_DBA.getClass().getSimpleName() + "Collator");
		int alphabetLengh = lc.getPrimOrderCount();
		assertEquals(BigInteger.valueOf(alphabetLengh), km.getOrderValue());

		km = getManager(2);
		assertEquals("", km.getValue());
		assertEquals(BigInteger.ZERO, km.getOrderValue());
		km.setValue("а");
		assertEquals(BigInteger.ONE, km.getOrderValue());
		km.setValue("аа");
		assertEquals(BigInteger.valueOf(2), km.getOrderValue());
		km.setValue("ая");
		assertEquals(BigInteger.valueOf(alphabetLengh + 1), km.getOrderValue());
		km.setValue("б");
		assertEquals(BigInteger.valueOf(alphabetLengh + 2), km.getOrderValue());

		km = getManager(12);
		km.setValue("яяяяяяяяяяяю");
		assertEquals(km.getOrderValue().add(BigInteger.valueOf(2)), km.cardinality());
	}

	@Test
	public void test6() throws CelestaException {
		VarcharFieldEnumerator km = new VarcharFieldEnumerator(DBA, 10);
		String[] test = { "А-2-А", "Ая", "ая", "ая Дачная" };
		BigInteger[] v = new BigInteger[test.length];
		for (int i = 0; i < test.length; i++) {
			km.setValue(test[i]);
			v[i] = km.getOrderValue();
		}
		// for (BigInteger o : v)
		// System.out.println(o.toString(16));
		// System.out.println("---");
		for (int i = 1; i < v.length; i++) {
			org.junit.Assert.assertTrue(v[i].compareTo(v[i - 1]) > 0);
		}

		for (int i = 0; i < test.length; i++) {
			km.setOrderValue(v[i]);
			assertEquals(test[i], km.getValue());
		}
	}

	@Test
	public void test7() throws CelestaException {
		VarcharFieldEnumerator km = new VarcharFieldEnumerator(DBA, 10);
		String[] test = { "Ё", "Е", "Еёее", "ее", "ееее", "ё",  "ёее" };
		BigInteger[] v = new BigInteger[test.length];
		for (int i = 0; i < test.length; i++) {
			km.setValue(test[i]);
			v[i] = km.getOrderValue();
		}

		for (int i = 1; i < v.length; i++) {
			org.junit.Assert.assertTrue(v[i].compareTo(v[i - 1]) > 0);
		}

		for (int i = 0; i < test.length; i++) {
			km.setOrderValue(v[i]);
			assertEquals(test[i], km.getValue());
		}
	}

	@Test
	public void test8() throws CelestaException {

		VarcharFieldEnumerator fe = new VarcharFieldEnumerator(DBA, 30);

		fe.setValue("Пётр");
		BigInteger k1 = fe.getOrderValue();

		fe.setValue("петр");
		BigInteger k2 = fe.getOrderValue();

		fe.setValue("");
		assertEquals(BigInteger.ZERO, fe.getOrderValue());

		assertTrue(k2.compareTo(k1) > 0);

		fe.setOrderValue(k1);
		assertEquals("Пётр", fe.getValue());

		fe.setOrderValue(k2);
		assertEquals("петр", fe.getValue());

		fe.setOrderValue(BigInteger.ZERO);
		assertEquals("", fe.getValue());

	}

	@Test
	public void test9() throws CelestaException {
		String val = "В начале июля, в чрезвычайно жаркое время, под вечер, один молодой человек вышел из своей каморки, которую нанимал от жильцов в С — м переулке, на улицу и медленно, как бы в нерешимости, отправился к ";
		VarcharFieldEnumerator e = new VarcharFieldEnumerator(DBA, 200);
		e.setValue(val);
		BigInteger ord = e.getOrderValue();

		VarcharFieldEnumerator e2 = new VarcharFieldEnumerator(DBA, 200);
		e2.setOrderValue(ord);
		assertEquals(val, e2.getValue());
	}
}

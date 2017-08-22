package ru.curs.lyra.grid;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.math.BigInteger;

import org.junit.Test;

public class KeyInterpolatorTest {

	@Test
	public void test1() {
		// FULL CODE COVERAGE FOR KeyApproximator!!
		KeyInterpolator ka = new KeyInterpolator(BigInteger.valueOf(7), BigInteger.valueOf(1000), 100, false);

		assertEquals(100, ka.getApproximateCount());

		// Три крайние точки
		assertEquals(BigInteger.valueOf(7), ka.getPoint(0));
		assertEquals(BigInteger.valueOf(8), ka.getPoint(1));
		assertEquals(BigInteger.valueOf(1000), ka.getPoint(99));

		assertEquals(0, ka.getApproximatePosition(BigInteger.valueOf(7)));
		assertEquals(1, ka.getApproximatePosition(BigInteger.valueOf(8)));
		assertEquals(99, ka.getApproximatePosition(BigInteger.valueOf(1000)));

		// тестируем округление в одну и в другую сторону.
		assertEquals(BigInteger.valueOf(231), ka.getPoint(23));
		assertEquals(BigInteger.valueOf(210), ka.getPoint(21));
		assertEquals(BigInteger.valueOf(160), ka.getPoint(16));
		assertEquals(BigInteger.valueOf(555), ka.getPoint(55));
		assertEquals(BigInteger.valueOf(544), ka.getPoint(54));

		assertEquals(23, ka.getApproximatePosition(BigInteger.valueOf(228)));
		assertEquals(59, ka.getApproximatePosition(BigInteger.valueOf(600)));
		assertEquals(61, ka.getApproximatePosition(BigInteger.valueOf(615)));

		assertEquals(2, ka.getPointsCount());

		ka.setPoint(BigInteger.valueOf(100), 10);
		ka.setPoint(BigInteger.valueOf(500), 50);
		ka.setPoint(BigInteger.valueOf(800), 60);

		assertEquals(10, ka.getApproximatePosition(BigInteger.valueOf(100)));
		assertEquals(50, ka.getApproximatePosition(BigInteger.valueOf(500)));
		assertEquals(51, ka.getApproximatePosition(BigInteger.valueOf(501)));
		assertEquals(60, ka.getApproximatePosition(BigInteger.valueOf(800)));
		assertEquals(55, ka.getApproximatePosition(BigInteger.valueOf(650)));

		assertEquals(0, ka.getApproximatePosition(BigInteger.valueOf(7)));
		assertEquals(1, ka.getApproximatePosition(BigInteger.valueOf(8)));
		assertEquals(0, ka.getApproximatePosition(BigInteger.valueOf(2)));

		assertEquals(99, ka.getApproximatePosition(BigInteger.valueOf(2000)));

		assertEquals(BigInteger.valueOf(100), ka.getPoint(10));
		assertEquals(BigInteger.valueOf(1000), ka.getPoint(150));

		assertEquals(5, ka.getPointsCount());

		assertEquals(BigInteger.valueOf(7), ka.getPoint(0));
		assertEquals(BigInteger.valueOf(634), ka.getPoint(55));
		assertEquals(BigInteger.valueOf(500), ka.getPoint(50));
		assertEquals(BigInteger.valueOf(1000), ka.getPoint(99));

		ka.setPoint(BigInteger.valueOf(900), 50);
		assertEquals(4, ka.getPointsCount());

		assertEquals(BigInteger.valueOf(909), ka.getPoint(55));
		assertEquals(BigInteger.valueOf(955), ka.getPoint(77));

		assertEquals(77, ka.getApproximatePosition(BigInteger.valueOf(955)));

		ka.setPoint(BigInteger.valueOf(950), 105);
		assertEquals(4, ka.getPointsCount());
		assertEquals(BigInteger.valueOf(900), ka.getPoint(50));
		assertEquals(BigInteger.valueOf(901), ka.getPoint(51));
		assertEquals(BigInteger.valueOf(927), ka.getPoint(80));
		assertEquals(BigInteger.valueOf(950), ka.getPoint(105));

		assertEquals(106, ka.getApproximateCount());

		ka.setPoint(BigInteger.valueOf(2), 0);
		assertEquals(4, ka.getPointsCount());
	}

	@Test
	public void test2() {
		KeyInterpolator ki = new KeyInterpolator(BigInteger.ZERO, BigInteger.valueOf(100), 101, false);
		BigInteger v = ki.getLeastAccurateValue();
		assertEquals(BigInteger.valueOf(50), v);

		ki.setPoint(BigInteger.valueOf(60), 60);

		v = ki.getLeastAccurateValue();
		assertEquals(BigInteger.valueOf(30), v);

		ki.setPoint(BigInteger.valueOf(30), 30);

		v = ki.getLeastAccurateValue();
		assertEquals(BigInteger.valueOf(80), v);
		ki.setPoint(BigInteger.valueOf(80), 80);

		v = ki.getLeastAccurateValue();
		assertEquals(BigInteger.valueOf(15), v);
		ki.setPoint(BigInteger.valueOf(15), 15);

		v = ki.getLeastAccurateValue();
		assertEquals(BigInteger.valueOf(45), v);
		v = ki.getLeastAccurateValue();
		assertEquals(BigInteger.valueOf(45), v);
		ki.setPoint(BigInteger.valueOf(45), 45);

		v = ki.getLeastAccurateValue();
		assertNull(v);
	}

	@Test
	public void test3() {
		KeyInterpolator ka = new KeyInterpolator(BigInteger.valueOf(0), BigInteger.valueOf(60), 7, false);
		assertEquals(0, ka.getApproximatePosition(BigInteger.valueOf(0)));
		assertEquals(1, ka.getApproximatePosition(BigInteger.valueOf(1)));
		assertEquals(2, ka.getApproximatePosition(BigInteger.valueOf(10)));
		assertEquals(3, ka.getApproximatePosition(BigInteger.valueOf(30)));
		assertEquals(6, ka.getApproximatePosition(BigInteger.valueOf(60)));
	}

	@Test
	public void test4() {
		KeyInterpolator ka = new KeyInterpolator(BigInteger.valueOf(0), BigInteger.valueOf(10), 10, false);

		ka.setPoint(BigInteger.valueOf(5), 6);
		assertEquals(0, ka.getClosestPosition(0));
		assertEquals(0, ka.getClosestPosition(1));
		assertEquals(0, ka.getClosestPosition(2));
		assertEquals(6, ka.getClosestPosition(3));
		assertEquals(6, ka.getClosestPosition(4));
		assertEquals(6, ka.getClosestPosition(5));
		assertEquals(6, ka.getClosestPosition(6));
		assertEquals(6, ka.getClosestPosition(7));
		assertEquals(9, ka.getClosestPosition(8));
		assertEquals(9, ka.getClosestPosition(9));

		// extreme case 1
		assertEquals(9, ka.getClosestPosition(100));

		// extreme case 2
		ka = new KeyInterpolator(BigInteger.valueOf(5), BigInteger.valueOf(10), 10, false);
		ka.setPoint(BigInteger.valueOf(5), 6);
		assertEquals(6, ka.getClosestPosition(0));
		assertEquals(6, ka.getClosestPosition(1));
	}

	@Test
	public void twoPointsCase() {
		KeyInterpolator ka = new KeyInterpolator(BigInteger.valueOf(2), BigInteger.valueOf(10), 2, false);
		assertEquals(0, ka.getApproximatePosition(BigInteger.valueOf(-10)));
		assertEquals(0, ka.getApproximatePosition(BigInteger.valueOf(0)));
		assertEquals(1, ka.getApproximatePosition(BigInteger.valueOf(3)));
		assertEquals(1, ka.getApproximatePosition(BigInteger.valueOf(7)));
		assertEquals(1, ka.getApproximatePosition(BigInteger.valueOf(20)));
	}
}

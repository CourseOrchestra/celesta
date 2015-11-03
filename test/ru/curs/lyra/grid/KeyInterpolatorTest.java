package ru.curs.lyra.grid;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.math.BigInteger;

import org.junit.Test;

public class KeyInterpolatorTest {

	@Test
	public void test1() {
		// FULL CODE COVERAGE FOR KeyApproximator!!
		KeyInterpolator ka = new KeyInterpolator(BigInteger.valueOf(7), BigInteger.valueOf(1000), 100);

		assertEquals(100, ka.getApproximateCount());

		assertEquals(BigInteger.valueOf(7), ka.getPoint(0));
		// тестируем округление в одну и в другую сторону.
		assertEquals(BigInteger.valueOf(228), ka.getPoint(22));

		assertEquals(22, ka.getApproximatePosition(BigInteger.valueOf(228)));

		assertEquals(BigInteger.valueOf(167), ka.getPoint(16));
		assertEquals(BigInteger.valueOf(1000), ka.getPoint(99));
		assertEquals(BigInteger.valueOf(559), ka.getPoint(55));

		assertEquals(2, ka.getPointsCount());

		ka.setPoint(BigInteger.valueOf(100), 10);
		ka.setPoint(BigInteger.valueOf(500), 50);
		ka.setPoint(BigInteger.valueOf(800), 60);

		assertEquals(50, ka.getApproximatePosition(BigInteger.valueOf(500)));
		assertEquals(60, ka.getApproximatePosition(BigInteger.valueOf(800)));
		assertEquals(55, ka.getApproximatePosition(BigInteger.valueOf(650)));

		assertEquals(0, ka.getApproximatePosition(BigInteger.valueOf(7)));
		assertEquals(0, ka.getApproximatePosition(BigInteger.valueOf(8)));
		assertEquals(0, ka.getApproximatePosition(BigInteger.valueOf(2)));

		assertEquals(99, ka.getApproximatePosition(BigInteger.valueOf(2000)));

		assertEquals(BigInteger.valueOf(100), ka.getPoint(10));
		assertEquals(BigInteger.valueOf(1000), ka.getPoint(150));

		assertEquals(5, ka.getPointsCount());

		assertEquals(BigInteger.valueOf(7), ka.getPoint(0));
		assertEquals(BigInteger.valueOf(650), ka.getPoint(55));
		assertEquals(BigInteger.valueOf(500), ka.getPoint(50));
		assertEquals(BigInteger.valueOf(1000), ka.getPoint(99));

		ka.setPoint(BigInteger.valueOf(900), 50);
		assertEquals(4, ka.getPointsCount());

		assertEquals(BigInteger.valueOf(910), ka.getPoint(55));
		assertEquals(BigInteger.valueOf(955), ka.getPoint(77));

		assertEquals(77, ka.getApproximatePosition(BigInteger.valueOf(955)));

		ka.setPoint(BigInteger.valueOf(950), 105);
		assertEquals(4, ka.getPointsCount());
		assertEquals(BigInteger.valueOf(950), ka.getPoint(105));

		assertEquals(106, ka.getApproximateCount());

		ka.setPoint(BigInteger.valueOf(2), 0);
		assertEquals(4, ka.getPointsCount());
	}

	@Test
	public void test2() {
		KeyInterpolator ki = new KeyInterpolator(BigInteger.ZERO, BigInteger.valueOf(100), 101);
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

}

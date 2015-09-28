package ru.curs.lyra.grid;

import static org.junit.Assert.assertEquals;

import java.math.BigInteger;

import org.junit.Test;

public class KeyApproximatorTest {

	@Test
	public void test1() {
		KeyApproximator ka = new KeyApproximator(BigInteger.valueOf(7),
				BigInteger.valueOf(1000), 100);
		assertEquals(BigInteger.valueOf(7), ka.getPoint(0));
		// тестируем округление в одну и в другую сторону.
		assertEquals(BigInteger.valueOf(228), ka.getPoint(22));
		assertEquals(BigInteger.valueOf(167), ka.getPoint(16));
		assertEquals(BigInteger.valueOf(1000), ka.getPoint(99));
		assertEquals(BigInteger.valueOf(559), ka.getPoint(55));

		assertEquals(2, ka.getPointsCount());

		ka.setPoint(BigInteger.valueOf(100), 10);
		ka.setPoint(BigInteger.valueOf(500), 50);
		ka.setPoint(BigInteger.valueOf(800), 60);

		assertEquals(5, ka.getPointsCount());

		assertEquals(BigInteger.valueOf(7), ka.getPoint(0));
		assertEquals(BigInteger.valueOf(650), ka.getPoint(55));
		assertEquals(BigInteger.valueOf(500), ka.getPoint(50));
		assertEquals(BigInteger.valueOf(1000), ka.getPoint(99));

		ka.setPoint(BigInteger.valueOf(900), 50);
		assertEquals(4, ka.getPointsCount());
		
		assertEquals(BigInteger.valueOf(910), ka.getPoint(55));
		assertEquals(BigInteger.valueOf(955), ka.getPoint(77));
		
		ka.setPoint(BigInteger.valueOf(950), 105);
		assertEquals(4, ka.getPointsCount());
		assertEquals(BigInteger.valueOf(950), ka.getPoint(105));
	}

}

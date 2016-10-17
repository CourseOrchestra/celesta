package ru.curs.lyra.grid;

import java.math.BigDecimal;
import java.math.BigInteger;

import ru.curs.celesta.CelestaException;

/**
 * Абстрактный класс нумератора ключа.
 */
public abstract class KeyEnumerator {
	private static final int MAX_BIT_LENGTH = 1022;

	/**
	 * Общий объём пространства значений первичного ключа (количества позиций).
	 */
	public abstract BigInteger cardinality();

	/**
	 * Порядок значения ключа в общем объёме пространства (значение от нуля до
	 * cardinality-1).
	 * 
	 * @throws CelestaException
	 *             in case it is impossible do define exact order value (e. g. a
	 *             collator error).
	 */
	public abstract BigInteger getOrderValue() throws CelestaException;

	/**
	 * Устанавливает порядковый номер ключа.
	 * 
	 * @param value
	 *            порядковый номер.
	 */
	public abstract void setOrderValue(BigInteger value);

	/**
	 * Устанавливает новое значение ключа.
	 * 
	 * @param value
	 *            значение ключа.
	 */
	public abstract void setValue(Object value);

	/**
	 * Возвращает значение поля.
	 */
	public abstract Object getValue();

	/**
	 * Возвращает позицию в виде действительного числа в диапазоне [0..1].
	 * 
	 * @throws CelestaException
	 *             in case it's impossible to get exact position (e. g. collator
	 *             error).
	 */
	public double getPosition() throws CelestaException {
		BigInteger order = getOrderValue();
		BigInteger maxOrder = cardinality().subtract(BigInteger.ONE);
		if (order.equals(BigInteger.ZERO)) {
			return 0.0;
		} else if (order.equals(maxOrder)) {
			return 1.0;
		} else {
			// first we are trying to simplify the fraction
			BigInteger gcd = order.gcd(maxOrder);
			order = order.divide(gcd);
			maxOrder = maxOrder.divide(gcd);

			// now we check if divisor is still too large
			// to be converted to double
			int blex = maxOrder.bitLength() - MAX_BIT_LENGTH;
			if (blex > 0) {
				order = order.shiftRight(blex);
				maxOrder = maxOrder.shiftRight(blex);
			}
			return order.doubleValue() / maxOrder.doubleValue();
		}
	}

	/**
	 * Устанавливает позицию в виде действительного числа от 0.0 до 1.0.
	 * 
	 * @param p
	 *            Позиция.
	 */
	public void setPosition(double p) {
		if (p < 0.0) {
			throw new IllegalArgumentException();
		} else if (p == 0.0) {
			setOrderValue(BigInteger.ZERO);
		} else if (p < 1.0) {
			BigDecimal c = new BigDecimal(cardinality(), 0);
			c = c.multiply(BigDecimal.valueOf(p));
			setOrderValue(c.toBigInteger());
		} else if (p == 1.0) {
			setOrderValue(cardinality().subtract(BigInteger.ONE));
		} else {
			throw new IllegalArgumentException();
		}
	}

}

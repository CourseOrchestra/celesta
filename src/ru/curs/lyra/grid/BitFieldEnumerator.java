package ru.curs.lyra.grid;

import java.math.BigInteger;

/**
 * Нумератор битового поля, входящего в первичный ключ.
 */
public final class BitFieldEnumerator extends KeyEnumerator {

	private boolean value;

	public BitFieldEnumerator() {

	}

	@Override
	public BigInteger cardinality() {
		return BigInteger.valueOf(2);
	}

	@Override
	public BigInteger getOrderValue() {
		if (value) {
			return BigInteger.ONE;
		} else {
			return BigInteger.ZERO;
		}
	}

	/**
	 * Текущее значение поля.
	 */
	@Override
	public Boolean getValue() {
		return value;
	}

	/**
	 * Устанавливает текущее значение поля.
	 * 
	 * @param value
	 *            Новое значение.
	 */
	@Override
	public void setValue(Object value) {
		if (value instanceof Boolean) {
			this.value = (Boolean) value;
		} else {
			throw new IllegalArgumentException();
		}
	}

	@Override
	public double getPosition() {
		return value ? 1.0 : 0.0;
	}

	@Override
	public void setOrderValue(BigInteger value) {
		if (value.equals(BigInteger.ZERO)) {
			this.value = false;
		} else if (value.equals(BigInteger.ONE)) {
			this.value = true;
		} else {
			throw new IllegalArgumentException();
		}
	}

}

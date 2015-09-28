package ru.curs.lyra.grid;

import java.math.BigInteger;

/**
 * Менеджер битового поля, входящего в первичный ключ.
 */
public final class BitFieldMgr extends KeyManager {

	private boolean value;

	public BitFieldMgr() {

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
	public boolean getValue() {
		return value;
	}

	/**
	 * Устанавливает текущее значение поля.
	 * 
	 * @param value
	 *            Новое значение.
	 */
	public void setValue(boolean value) {
		this.value = value;
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

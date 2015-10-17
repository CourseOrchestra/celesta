package ru.curs.lyra.grid;

import java.math.BigInteger;

/**
 * Менеджер составного первичного ключа.
 */
public final class CompositeKeyManager extends KeyManager {
	private final KeyManager[] keys;

	public CompositeKeyManager(KeyManager... kfm) {
		keys = kfm;
	}

	@Override
	public BigInteger cardinality() {
		if (keys.length == 0) {
			return BigInteger.ONE;
		} else {
			BigInteger result = keys[0].cardinality();
			for (int i = 1; i < keys.length; i++)
				result = result.multiply(keys[i].cardinality());
			return result;
		}
	}

	@Override
	public BigInteger getOrderValue() {
		if (keys.length == 0) {
			return BigInteger.ZERO;
		} else {
			BigInteger result = keys[0].getOrderValue();
			for (int i = 1; i < keys.length; i++) {
				KeyManager km = keys[i];
				result = result.multiply(km.cardinality()).add(km.getOrderValue());
			}
			return result;
		}
	}

	@Override
	public void setOrderValue(BigInteger value) {
		if (keys.length == 0)
			return;

		BigInteger[] c = new BigInteger[keys.length];
		BigInteger n = BigInteger.ONE;
		int i = keys.length - 1;
		while (true) {
			c[i] = n;
			if (i == 0)
				break;
			n = n.multiply(keys[i].cardinality());
			i--;
		}

		BigInteger[] vr;
		BigInteger v = value;
		for (i = 0; i < c.length - 1; i++) {
			vr = v.divideAndRemainder(c[i]);
			v = vr[1];
			keys[i].setOrderValue(vr[0]);
		}
		keys[c.length - 1].setOrderValue(v);
	}

	@Override
	public void setValue(Object value) {
		// do nothing, no sense for this type of KeyManager
	}

	@Override
	public Object getValue() {
		// return nothing
		return null;
	}
}

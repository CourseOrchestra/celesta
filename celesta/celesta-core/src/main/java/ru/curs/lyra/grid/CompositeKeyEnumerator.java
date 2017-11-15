package ru.curs.lyra.grid;

import java.math.BigInteger;
import java.util.Optional;

import ru.curs.celesta.CelestaException;

/**
 * Нумератор составного ключа.
 */
public final class CompositeKeyEnumerator extends KeyEnumerator {
	private final KeyEnumerator[] keys;

	public CompositeKeyEnumerator(KeyEnumerator... kfm) {
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
	public BigInteger getOrderValue() throws CelestaException {
		if (keys.length == 0) {
			return BigInteger.ZERO;
		} else {
			BigInteger result = keys[0].getOrderValue();
			for (int i = 1; i < keys.length; i++) {
				KeyEnumerator km = keys[i];
				result = result.multiply(km.cardinality()).add(km.getOrderValue());
			}
			return result;
		}
	}

	@Override
	public void setOrderValue(BigInteger value) {
		if (keys.length == 0)
			return;

		BigInteger v = value;
		BigInteger[] vr;
		for (int i = keys.length - 1; i >= 0; i--) {
			vr = v.divideAndRemainder(keys[i].cardinality());
			keys[i].setOrderValue(vr[1]);
			v = vr[0];
		}
	}

	@Override
	public void setValue(Object value) {
		// do nothing, no sense for this type of KeyManager
	}

	@Override
	public Object getValue() {
		StringBuilder sb = new StringBuilder("(");
		for (int i = 0; i < keys.length; i++) {
			if (i > 0)
				sb.append(";");
			sb.append(Optional.ofNullable(keys[i].getValue()).map(Object::toString).orElse("NULL"));
		}
		sb.append(")");
		return sb.toString();
	}
}

package ru.curs.lyra.grid;

import java.math.BigInteger;

import ru.curs.celesta.CelestaException;

/**
 * Numerator for a nullable field.
 */
public abstract class NullableFieldEnumerator extends KeyEnumerator {

	private final KeyEnumerator parent;
	private boolean valueIsNull = true;

	NullableFieldEnumerator(KeyEnumerator parent) {
		this.parent = parent;
	}

	@Override
	public BigInteger cardinality() {
		return getParent().cardinality().add(BigInteger.ONE);
	}

	@Override
	public void setValue(Object value) {
		if (value == null) {
			setValueIsNull(true);
		} else {
			setValueIsNull(false);
			getParent().setValue(value);
		}
	}

	@Override
	public Object getValue() {
		return isValueIsNull() ? null : getParent().getValue();
	}

	boolean isValueIsNull() {
		return valueIsNull;
	}

	void setValueIsNull(boolean valueIsNull) {
		this.valueIsNull = valueIsNull;
	}

	KeyEnumerator getParent() {
		return parent;
	}

	/**
	 * Create NullableFieldEnumerator either for NULLS FIRST or NULLS LAST
	 * database behaviour.
	 * 
	 * @param nullsFirst
	 *            true for NULLS FIRST.
	 * @param parent
	 *            parent enumerator.
	 * 
	 */
	public static NullableFieldEnumerator create(boolean nullsFirst, KeyEnumerator parent) {
		return nullsFirst ? new NullsFirst(parent) : new NullsLast(parent);
	}

}

/**
 * NullableFieldEnumerator for NULLS FIRST.
 */
class NullsFirst extends NullableFieldEnumerator {
	NullsFirst(KeyEnumerator parent) {
		super(parent);
	}

	@Override
	public BigInteger getOrderValue() throws CelestaException {
		return isValueIsNull() ? BigInteger.ZERO : getParent().getOrderValue().add(BigInteger.ONE);
	}

	@Override
	public void setOrderValue(BigInteger value) {
		if (BigInteger.ZERO.equals(value)) {
			setValueIsNull(true);
		} else {
			setValueIsNull(false);
			getParent().setOrderValue(value.subtract(BigInteger.ONE));
		}
	}
}

/**
 * NullableFieldEnumerator for NULLS LAST.
 */
class NullsLast extends NullableFieldEnumerator {
	NullsLast(KeyEnumerator parent) {
		super(parent);
	}

	@Override
	public BigInteger getOrderValue() throws CelestaException {
		return isValueIsNull() ? getParent().cardinality() : getParent().getOrderValue();
	}

	@Override
	public void setOrderValue(BigInteger value) {
		if (getParent().cardinality().equals(value)) {
			setValueIsNull(true);
		} else {
			setValueIsNull(false);
			getParent().setOrderValue(value);
		}
	}

}

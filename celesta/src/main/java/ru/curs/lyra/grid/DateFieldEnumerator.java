package ru.curs.lyra.grid;

import java.math.BigInteger;
import java.util.Date;

import ru.curs.celesta.CelestaException;

public class DateFieldEnumerator extends KeyEnumerator {

	private Date value;
	private static final BigInteger CARD = BigInteger.valueOf(Long.MAX_VALUE)
			.subtract(BigInteger.valueOf(Long.MIN_VALUE));

	@Override
	public BigInteger cardinality() {
		return CARD;
	}

	@Override
	public BigInteger getOrderValue() throws CelestaException {
		return BigInteger.valueOf(value.getTime());
	}

	@Override
	public void setOrderValue(BigInteger value) {
		this.value = new Date(value.longValueExact());
	}

	@Override
	public void setValue(Object value) {
		if (value instanceof Date) {
			this.value = (Date) value;
		} else {
			throw new IllegalArgumentException();
		}
	}

	@Override
	public Date getValue() {
		return value;
	}

}

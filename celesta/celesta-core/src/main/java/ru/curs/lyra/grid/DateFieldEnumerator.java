package ru.curs.lyra.grid;

import java.math.BigInteger;
import java.util.Date;

import ru.curs.celesta.CelestaException;

public class DateFieldEnumerator extends KeyEnumerator {

	private Date value;

	private static BigInteger min = BigInteger.valueOf(-2208988800000L); // 1900-01-01
	private static BigInteger max = BigInteger.valueOf( 4102444800000L); // 2100-01-01

	private static final BigInteger CARD = max.subtract(min).add(BigInteger.ONE);

	@Override
	public BigInteger cardinality() {
		return CARD;
	}

	@Override
	public BigInteger getOrderValue() throws CelestaException {
		return BigInteger.valueOf(value.getTime()).subtract(min);
	}

	@Override
	public void setOrderValue(BigInteger value) {
		this.value = new Date(value.add(min).longValueExact());
	}

	@Override
	public void setValue(Object value) {
		if (value instanceof Date) {
			Date d = (Date) value;
			long t = d.getTime();
			if (t < min.longValue() || t > max.longValue())
				throw new IllegalArgumentException();
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

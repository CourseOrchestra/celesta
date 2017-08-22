package ru.curs.lyra.grid;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * Кусочно-линейная аппроксимация распределения значений первичного ключа.
 */
public class KeyInterpolator {

	private static final int MIN_GAP_QUOTIENT = 4;
	private static final int MIN_GAP_VALUE = 10;

	private final TreeMap<Integer, BigInteger> data = new TreeMap<>();
	private final boolean descending;

	// Least accurate value cache and its validity flag.
	private BigInteger leastAccurateValue;
	private boolean isLAVValid;

	public KeyInterpolator(BigInteger minOrd, BigInteger maxOrd, int count, boolean descending) {
		this.descending = descending;
		data.put(0, negateIfDesc(minOrd));
		if (count > 0) {
			data.put(count - 1, negateIfDesc(maxOrd));
			// self-testing count/maxOrd consistency for extremal cases
			if (count == 1 && !minOrd.equals(maxOrd))
				throw new IllegalArgumentException();
		} else if (count < 0) {
			throw new IllegalArgumentException();
		}
		isLAVValid = false;

	}

	/**
	 * Установка соответствия номера записи значению первичного ключа.
	 * 
	 * @param ord
	 *            Порядковое значение первичного ключа.
	 * @param count
	 *            Номер записи.
	 */
	public void setPoint(BigInteger ord, int count) {
		if (count < 0)
			throw new IllegalArgumentException();
		final BigInteger neword = negateIfDesc(ord);

		synchronized (this) {
			isLAVValid = false;
			// System.out.printf("+(%d:%s)%n", count, ord.toString());
			Entry<Integer, BigInteger> e;
			data.put(count, neword);
			int c = count;
			// Discarding non-congruent points
			while ((e = data.lowerEntry(c)) != null) {
				if (e.getValue().compareTo(neword) >= 0) {
					c = e.getKey();
					data.remove(c);
				} else {
					break;
				}
			}
			c = count;
			while ((e = data.higherEntry(c)) != null) {
				if (e.getValue().compareTo(neword) <= 0) {
					c = e.getKey();
					data.remove(c);
				} else {
					break;
				}
			}
		}

		// TODO: выбрасывать ненужные (не уточняющие) точки
	}

	private BigInteger negateIfDesc(BigInteger ord) {
		return ord == null ? null : (descending ? ord.negate() : ord);
	}

	/**
	 * Exact (not approximated) point or null if no such point exist in
	 * approximator.
	 * 
	 * @param count
	 *            Record's number.
	 * 
	 */
	public BigInteger getExactPoint(int count) {
		return negateIfDesc(data.get(count));
	}

	/**
	 * The closest known position to to the given one.
	 * 
	 * @param count
	 *            The ordinal number of record.
	 */
	public int getClosestPosition(int count) {
		if (count < 0)
			throw new IllegalArgumentException();
		synchronized (this) {
			if (data.isEmpty())
				throw new IllegalStateException();
			Integer floor = data.floorKey(count);
			int e0 = floor == null ? data.firstKey() : floor;
			if (e0 == count)
				return e0;
			Integer ceiling = data.ceilingKey(count);
			int e1 = ceiling == null ? data.lastKey() : ceiling;
			return (count - e0 < e1 - count) ? e0 : e1;
		}
	}

	/**
	 * Примерное значение первичного ключа по данному номеру.
	 * 
	 * @param count
	 *            Номер записи.
	 */
	public BigInteger getPoint(int count) {
		if (count < 0)
			throw new IllegalArgumentException();
		Entry<Integer, BigInteger> e0, e1;
		synchronized (this) {
			e0 = data.floorEntry(count);
			if (e0.getKey() == count)
				return negateIfDesc(e0.getValue());
			e1 = data.ceilingEntry(count);
			// when count > maxcount
			if (e1 == null)
				return negateIfDesc(data.lastEntry().getValue());
		}
		BigInteger result = (e1.getValue().subtract(e0.getValue()).subtract(BigInteger.ONE))
				.multiply(BigInteger.valueOf(count - e0.getKey() - 1));
		BigInteger delta = BigInteger.valueOf(e1.getKey() - e0.getKey() - 1);
		result = e0.getValue().add(divideAndRound(result, delta)).add(BigInteger.ONE);
		return negateIfDesc(result);
	}

	private static BigInteger divideAndRound(BigInteger divident, BigInteger divisor) {
		BigInteger[] qr = divident.divideAndRemainder(divisor);
		if (qr[1].shiftLeft(1).compareTo(divisor) > 0) {
			return qr[0].add(BigInteger.ONE);
		} else {
			return qr[0];
		}
	}

	/**
	 * Количество имеющихся точек в таблице.
	 */
	public int getPointsCount() {
		return data.size();
	}

	/**
	 * Returns an (approximate) records count.
	 */
	public int getApproximateCount() {
		return data.lastEntry().getKey() + 1;
	}

	/**
	 * Returns an (approximate) position of a key in a set using inverse
	 * interpolation.
	 * 
	 * @param key
	 *            Key ordinal value.
	 */
	public int getApproximatePosition(BigInteger key) {
		final BigInteger newkey = negateIfDesc(key);
		int cmin = 0;
		int cmid;
		synchronized (this) {
			int cmax = data.lastEntry().getKey();
			while (cmax > cmin) {
				cmid = (cmax + cmin) >> 1;
				if (cmid == cmin)
					cmid = cmax;
				Entry<Integer, BigInteger> ceiling = data.ceilingEntry(cmid);
				int delta = ceiling.getValue().compareTo(newkey);
				if (delta == 0) {
					return ceiling.getKey();
				} else if (delta < 0) {
					// Ceiling is strictly lower than key: we should try higher
					// cmin
					cmin = ceiling.getKey();
				} else {
					// Ceiling is strictly greater than key!
					Entry<Integer, BigInteger> lower = data.lowerEntry(cmid);
					delta = lower.getValue().compareTo(newkey);
					if (delta == 0)
						return lower.getKey();
					else if (delta > 0) {
						// Lower entry is strictly greater than key: we should
						// try
						// lower cmax
						cmax = lower.getKey();
					} else {
						// Lower entry is strictly lower,
						// Ceiling is strictly greater: interpolation
						int d = 1 + divideAndRound(
								BigInteger.valueOf(ceiling.getKey() - lower.getKey() - 1)
										.multiply(newkey.subtract(lower.getValue()).subtract(BigInteger.ONE)),
								ceiling.getValue().subtract(lower.getValue()).subtract(BigInteger.ONE)).intValue();
						return lower.getKey() + d;
					}
				}
			}
		}
		return cmin;
	}

	/**
	 * Gets the value that corresponds to the center of the biggest gap in this
	 * interpolation table.
	 * 
	 * Returns null if there is no gap big enough.
	 */
	public synchronized BigInteger getLeastAccurateValue() {
		if (isLAVValid)
			return negateIfDesc(leastAccurateValue);

		isLAVValid = true;
		// only one point, nothing to talk about
		if (data.size() < 2) {
			leastAccurateValue = null;
			return null;
		}

		// looking for biggest gap in a table
		int deltaMax = 0;
		int cMax = data.lastKey();

		int deltaMin = cMax / MIN_GAP_QUOTIENT;
		if (deltaMin < MIN_GAP_VALUE)
			deltaMin = MIN_GAP_VALUE;

		Iterator<Entry<Integer, BigInteger>> i = data.entrySet().iterator();
		Entry<Integer, BigInteger> c = i.next();
		Entry<Integer, BigInteger> cPrev;
		BigInteger v1 = BigInteger.ZERO;
		BigInteger v2 = BigInteger.ZERO;
		do {
			cPrev = c;
			c = i.next();
			int d = c.getKey() - cPrev.getKey();
			if (d > deltaMax) {
				cMax += deltaMax;
				deltaMax = d;
				cMax -= deltaMax;
				v1 = cPrev.getValue();
				v2 = c.getValue();
			}
		} while (c.getKey() < cMax);

		if (deltaMax > deltaMin) {
			leastAccurateValue = v1.add(v2).shiftRight(1);
		} else {
			leastAccurateValue = null;
		}

		return negateIfDesc(leastAccurateValue);
	}

	/**
	 * Resets the interpolator when all records are deleted.
	 */
	public synchronized void resetToEmptyTable() {
		data.clear();
		data.put(0, BigInteger.ZERO);
		isLAVValid = false;
	}
}

package ru.curs.lyra.grid;

import java.math.BigInteger;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * Кусочно-линейная аппроксимация распределения значений первичного ключа.
 */
public class KeyInterpolator {
	private final TreeMap<Integer, BigInteger> data = new TreeMap<>();

	public KeyInterpolator(BigInteger minOrd, BigInteger maxOrd, int count) {
		data.put(0, minOrd);
		data.put(count - 1, maxOrd);
	}

	/**
	 * Установка соответствия номера записи значению первичного ключа.
	 * 
	 * @param ord
	 *            Порядковое значение первичного ключа.
	 * @param count
	 *            Номер записи.
	 */
	public synchronized void setPoint(BigInteger ord, int count) {
		// System.out.printf("+(%d:%s)%n", count, ord.toString());
		Entry<Integer, BigInteger> e;
		data.put(count, ord);
		int c = count;
		// Discarding non-congruent points
		while ((e = data.lowerEntry(c)) != null) {
			if (e.getValue().compareTo(ord) >= 0) {
				c = e.getKey();
				data.remove(c);
			} else {
				break;
			}
		}
		c = count;
		while ((e = data.higherEntry(c)) != null) {
			if (e.getValue().compareTo(ord) <= 0) {
				c = e.getKey();
				data.remove(c);
			} else {
				break;
			}
		}

		// TODO: выбрасывать ненужные (не уточняющие) точки
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
		return data.get(count);
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
		Entry<Integer, BigInteger> e0 = data.floorEntry(count);
		if (e0.getKey() == count)
			return e0.getValue();
		Entry<Integer, BigInteger> e1 = data.ceilingEntry(count);
		// when count > maxcount
		if (e1 == null)
			return data.lastEntry().getValue();

		BigInteger result = e1.getValue().subtract(e0.getValue()).multiply(BigInteger.valueOf(count - e0.getKey()));

		BigInteger delta = BigInteger.valueOf(e1.getKey() - e0.getKey());
		result = e0.getValue().add(divideAndRound(result, delta));
		return result;
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
	 * Returns an (approximate) position of a key in a set.
	 * 
	 * @param key
	 *            Key ordinal value.
	 */
	public int getApproximatePosition(BigInteger key) {
		int cmax = data.lastEntry().getKey();
		int cmin = 0;
		int cmid;
		while (cmax != cmin) {
			cmid = (cmax + cmin) >> 1;
			Entry<Integer, BigInteger> ceiling = data.ceilingEntry(cmid);
			int delta = ceiling.getValue().compareTo(key);
			if (delta == 0) {
				return ceiling.getKey();
			} else if (delta < 0) {
				// Ceiling is strictly lower than key: we should try higher cmin
				cmin = ceiling.getKey();
			} else {
				// Ceiling is strictly greater than key!
				Entry<Integer, BigInteger> lower = data.lowerEntry(cmid);
				delta = lower.getValue().compareTo(key);
				if (delta == 0)
					return lower.getKey();
				else if (delta > 0) {
					// Lower entry is strictly greater than key: we should try
					// lower cmax
					cmax = lower.getKey();
				} else {
					// Lower entry is strictly lower,
					// Ceiling is strictly greater: interpolation
					int d = divideAndRound(BigInteger.valueOf(ceiling.getKey() - lower.getKey())
							.multiply(key.subtract(lower.getValue())), ceiling.getValue().subtract(lower.getValue()))
									.intValue();
					return lower.getKey() + d;
				}
			}
		}
		return cmin;
	}

	public TreeMap<Integer, BigInteger> getData() {
		return data;
	}
}

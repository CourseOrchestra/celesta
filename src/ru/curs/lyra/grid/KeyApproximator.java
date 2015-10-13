package ru.curs.lyra.grid;

import java.math.BigInteger;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * Кусочно-линейная аппроксимация распределения значений первичного ключа.
 */
public class KeyApproximator {
	private final TreeMap<Integer, BigInteger> data = new TreeMap<>();

	public KeyApproximator(BigInteger minOrd, BigInteger maxOrd, int count) {
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
	public void setPoint(BigInteger ord, int count) {
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
		if (e1.getKey() == count)
			return e1.getValue();

		BigInteger result = e1.getValue().subtract(e0.getValue()).multiply(BigInteger.valueOf(count - e0.getKey()));

		BigInteger delta = BigInteger.valueOf(e1.getKey() - e0.getKey());
		BigInteger[] cr = result.divideAndRemainder(delta);
		// Rounding to the closest integer using remainder!
		if (cr[1].shiftLeft(1).compareTo(delta) > 0) {
			result = e0.getValue().add(cr[0]).add(BigInteger.ONE);
		} else {
			result = e0.getValue().add(cr[0]);
		}

		return result;
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
}

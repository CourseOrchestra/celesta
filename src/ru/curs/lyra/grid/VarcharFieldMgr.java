package ru.curs.lyra.grid;

import java.math.BigInteger;
import java.util.Arrays;

/**
 * Менеджер ключа с типом varchar.
 * 
 */
public class VarcharFieldMgr extends KeyManager {

	static final char[] DEFAULT_ALPHABET;
	static final int[] EMPTY_STRING = new int[0];

	static {

		// DEFAULT_ALPHABET = ("
		// !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}"
		// +
		// "~¡¢£¤¥¦§©«¬®°±µ¶·»¿ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖ×ØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõö÷øùúûüýþÿŒœŠšŸŽžЁЂЃЄЅІЇЈЉЊЋЌЎЏ"
		// +
		// "АБВГДЕЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯабвгдежзийклмнопрстуфхцчшщъыьэюяёђѓєѕіїјљњћќўџҐґ–—‘’‚“”„†‡•…‰‹›€№™")
		// .toCharArray();

		DEFAULT_ALPHABET = ("!\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}"
				+ "~¡¢£¤¥¦§©«¬®°±µ¶·»АБВГДЕЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯабвгдежзийклмнопрстуфхцчшщъыьэюяё№™")
						.toCharArray();

		// DEFAULT_ALPHABET = ("0123456789").toCharArray();

	}

	private final char[] alphabet;

	private final int m;
	private final BigInteger[] q;

	private int[] min;
	private int[] max;
	private BigInteger card;
	private BigInteger minOrd;

	private String value = "";

	public VarcharFieldMgr(int m) {
		this(DEFAULT_ALPHABET, m);
	}

	public VarcharFieldMgr(char[] a, int m) {
		this(a, m, true);
		this.min = EMPTY_STRING;
		this.max = new int[m];
		for (int i = 0; i < m; i++)
			this.max[i] = a.length - 1;
		minOrd = ord(min);
		card = ord(max).subtract(ord(min)).add(BigInteger.ONE);
	}

	public VarcharFieldMgr(String min, String max, int m) {
		this(DEFAULT_ALPHABET, min, max, m);
	}

	public VarcharFieldMgr(char[] a, String min, String max, int m) {
		this(a, m, true);
		setBounds(min, max);
	}

	private VarcharFieldMgr(char[] a, int m, boolean setup) {
		if (m <= 0)
			throw new IllegalArgumentException();
		// Setting up alphabet
		Arrays.sort(a);
		alphabet = a;
		BigInteger alphabetLength = BigInteger.valueOf(a.length);

		this.m = m;
		this.q = new BigInteger[m];
		BigInteger ai = BigInteger.ONE;
		BigInteger s = BigInteger.ZERO;
		for (int i = m - 1; i >= 0; i--) {
			s = s.add(ai);
			q[i] = s;
			ai = ai.multiply(alphabetLength);
		}
	}

	/**
	 * Установка минимального и максимального значения поля.
	 * 
	 * @param min
	 *            минимальное значение.
	 * @param max
	 *            максимальное значение.
	 */
	public void setBounds(String min, String max) {
		this.min = toArray(min);
		this.max = toArray(max);
		minOrd = ord(this.min);
		card = ord(this.max).subtract(ord(this.min)).add(BigInteger.ONE);
	}

	private int[] toArray(String str) {
		if (str.isEmpty())
			return EMPTY_STRING;
		int[] result = new int[str.length()];
		for (int i = 0; i < str.length(); i++)
			result[i] = Math.abs(Arrays.binarySearch(alphabet, str.charAt(i)));
		return result;
	}

	private BigInteger ord(int[] s) {
		BigInteger result = BigInteger.valueOf(s.length);
		for (int i = 0; i < s.length; i++)
			result = result.add(q[i].multiply(BigInteger.valueOf(s[i])));
		return result;
	}

	@Override
	public BigInteger cardinality() {
		return card;
	}

	@Override
	public BigInteger getOrderValue() {
		int[] arr = toArray(value);
		return ord(arr).subtract(minOrd);
	}

	/**
	 * Текущее значение поля.
	 */
	public String getValue() {
		return value;
	}

	/**
	 * Установка значения поля.
	 * 
	 * @param value
	 *            Значение (строка).
	 */
	@Override
	public void setValue(Object value) {
		if (value instanceof String) {
			this.value = (String) value;
		} else {
			throw new IllegalArgumentException();
		}
	}

	@Override
	public void setOrderValue(BigInteger value) {
		BigInteger r = value.add(minOrd);
		if (r.equals(BigInteger.ZERO)) {
			this.value = "";
			return;
		}
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < m; i++) {
			r = r.subtract(BigInteger.ONE);
			BigInteger[] cr = r.divideAndRemainder(q[i]);
			r = cr[1];
			int c = cr[0].intValue();
			if (c < 0)
				c = 0;
			else if (c >= alphabet.length)
				c = alphabet.length - 1;
			sb.append(alphabet[(int) c]);
			if (r.equals(BigInteger.ZERO))
				break;
		}
		this.value = sb.toString();
	}
}

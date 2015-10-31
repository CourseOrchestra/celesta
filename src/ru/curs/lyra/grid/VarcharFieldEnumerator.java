package ru.curs.lyra.grid;

import java.math.BigInteger;

import ru.curs.celesta.CelestaException;

/**
 * Нумератор ключа с типом varchar.
 * 
 */
public class VarcharFieldEnumerator extends KeyEnumerator {

	/**
	 * Postgres collation rules.
	 */
	public static final String POSTGRES;
	static final int[][] EMPTY_STRING = new int[0][0];

	static {
		POSTGRES = "<'''<'-'<'–'<'—'<' '<'!'<'\"'<'#'<'$'<'%'<'&'<'('<')'<'*'<','<'.'<'/'<':'<';'"
				+ "<'?'<'@'<'['<'\\'<']'<'^'<'_'<'`'<'{'<'|'<'}'<'~'<'¦'<'‘'<'’'<'‚'<'“'<'”'<'„'<'‹'<'›'<'+'"
				+ "<'<'<'='<'>'<'«'<'»'<'§'<'©'<'¬'<'®'<'°'<'†'<'‡'<'•'<'‰'<0<1<2<3<4<5<6<7<8<9"
				+ "<a,A<b,B<c,C<d,D<e,E<f,F<g,G<h,H<i,I<j,J<k,K<l,L<m,M<n,N;№<o,O<p,P<q,Q<r,R<s,S<t,T<™<u,U<v,V<w,W<x,X<y,Y<z,Z"
				+ "<а,А<б,Б<в,В<г,Г<д,Д<е,Е;ё,Ё<ж,Ж<з,З<и,И<й,Й<к,К<л,Л<м,М<н,Н<о,О<п,П<р,Р<с,С<т,Т<у,У<ф,Ф<х,Х<ц,Ц<ч,Ч<ш,Ш<щ,Щ"
				+ "<ъ,Ъ<ы,Ы<ь,Ь<э,Э<ю,Ю<я,Я";
	}

	private final LyraCollator collator;

	private final int m;
	private final BigInteger[][] q;
	private final BigInteger c2;
	private final BigInteger c3;

	private int[][] min;
	private int[][] max;
	private BigInteger card;
	private BigInteger minOrd;

	private String value = "";

	public VarcharFieldEnumerator(int m) {
		this(POSTGRES, m);
	}

	public VarcharFieldEnumerator(String rules, int m) {
		this(rules, m, true);
		this.min = EMPTY_STRING;
		this.max = new int[m][3];
		int p = collator.getPrimOrderCount() - 1;
		int s = collator.getSecOrderCount() - 1;
		int t = collator.getTerOrderCount() - 1;
		for (int i = 0; i < m; i++) {
			this.max[i][0] = p;
			this.max[i][1] = s;
			this.max[i][2] = t;
		}
		minOrd = ord(min);
		card = ord(max).subtract(ord(min)).add(BigInteger.ONE);

	}

	public VarcharFieldEnumerator(String min, String max, int m) throws CelestaException {
		this(POSTGRES, min, max, m);
	}

	public VarcharFieldEnumerator(String rules, String min, String max, int m) throws CelestaException {
		this(rules, m, true);
		setBounds(min, max);
	}

	private VarcharFieldEnumerator(String rules, int m, boolean setup) {
		if (m <= 0)
			throw new IllegalArgumentException();
		// Setting up collator
		collator = LyraCollator.getInstance(rules);

		BigInteger[] count = { BigInteger.valueOf(collator.getPrimOrderCount()),
				BigInteger.valueOf(collator.getSecOrderCount()), BigInteger.valueOf(collator.getTerOrderCount()) };
		this.m = m;
		this.q = new BigInteger[m][3];
		for (int j = 0; j < 3; j++) {
			q[m - 1][j] = BigInteger.ONE;
			for (int i = m - 2; i >= 0; i--) {
				q[i][j] = q[i + 1][j].multiply(count[j]).add(j == 0 ? BigInteger.ONE : BigInteger.ZERO);
			}
		}
		c2 = q[0][1].multiply(count[1]);
		c3 = q[0][2].multiply(count[2]); 
	}

	/**
	 * Установка минимального и максимального значения поля.
	 * 
	 * @param min
	 *            минимальное значение.
	 * @param max
	 *            максимальное значение.
	 * @throws CelestaException
	 *             в случае, если строка содержит неизвестный символ.
	 */
	public void setBounds(String min, String max) throws CelestaException {
		this.min = toArray(min);
		this.max = toArray(max);
		minOrd = ord(this.min);
		card = ord(this.max).subtract(ord(this.min)).add(BigInteger.ONE);
	}

	private int[][] toArray(String str) throws CelestaException {
		if (str.isEmpty())
			return EMPTY_STRING;
		int[][] result = new int[str.length()][3];
		LyraCollationElementIterator i = collator.getCollationElementIterator(str);
		int j = 0;
		while (i.next()) {
			result[j][0] = i.primaryOrder();
			result[j][1] = i.secondaryOrder();
			result[j][2] = i.tertiaryOrder();
			j++;
		}
		return result;
	}

	private BigInteger atomicOrd(int[][] s, int o) {
		BigInteger result = o == 0 ? BigInteger.valueOf(s.length) : BigInteger.ZERO;
		for (int i = 0; i < s.length; i++)
			result = result.add(q[i][o].multiply(BigInteger.valueOf(s[i][o])));
		return result;
	}

	private BigInteger ord(int[][] s) {
		BigInteger[] o = new BigInteger[3];
		for (int i = 0; i < 3; i++)
			o[i] = atomicOrd(s, i);
		return o[0].multiply(c2).add(o[1]).multiply(c3).add(o[2]);
	}

	@Override
	public BigInteger cardinality() {
		return card;
	}

	@Override
	public BigInteger getOrderValue() throws CelestaException {
		int[][] arr;
		arr = toArray(value);
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
		// check if we are dealing with empty string
		BigInteger r = value.add(minOrd);
		if (r.equals(BigInteger.ZERO)) {
			this.value = "";
			return;
		}
		// split the ordinal value into three components
		BigInteger[] components = new BigInteger[3];
		BigInteger[] cr = r.divideAndRemainder(c3);
		components[2] = cr[1];
		cr = cr[0].divideAndRemainder(c2);
		components[1] = cr[1];
		components[0] = cr[0];
		// fill the array of collation elements
		int[][] arr = new int[m][3];
		for (int j = 0; j < 3; j++) {
			r = components[j];
			for (int i = 0; i < m; i++) {
				r = r.subtract(j == 0 ? BigInteger.ONE : BigInteger.ZERO);
				cr = r.divideAndRemainder(q[i][j]);
				r = cr[1];
				int c = cr[0].intValue();
				if (c < 0)
					c = 0;
				else if (c >= collator.getPrimOrderCount())
					c = collator.getPrimOrderCount() - 1;
				arr[i][j] = c;
				if (r.equals(BigInteger.ZERO)) {
					if (j == 0 && i + 1 < m)
						arr[i + 1][j] = -1;
					break;
				}
			}
		}
		// now reconstruct the string
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < m; i++) {
			if (arr[i][0] < 0)
				break;
			sb.append(collator.getElement(arr[i][0], arr[i][1], arr[i][2]));
		}
		this.value = sb.toString();
	}
}

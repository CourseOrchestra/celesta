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
				+ "<а,А<б,Б<в,В<г,Г<д,Д<е,Е<ё,Ё<ж,Ж<з,З<и,И<й,Й<к,К<л,Л<м,М<н,Н<о,О<п,П<р,Р<с,С<т,Т<у,У<ф,Ф<х,Х<ц,Ц<ч,Ч<ш,Ш<щ,Щ"
				+ "<ъ,Ъ<ы,Ы<ь,Ь<э,Э<ю,Ю<я,Я";
	}

	private final LyraCollator collator;

	private final int m;
	private final BigInteger[] q;

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
		minOrd = atomicOrd(min);
		card = atomicOrd(max).subtract(atomicOrd(min)).add(BigInteger.ONE);

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

		BigInteger alphabetLength = BigInteger.valueOf(collator.getPrimOrderCount());
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
	 * @throws CelestaException
	 *             в случае, если строка содержит неизвестный символ.
	 */
	public void setBounds(String min, String max) throws CelestaException {
		this.min = toArray(min);
		this.max = toArray(max);
		minOrd = atomicOrd(this.min);
		card = atomicOrd(this.max).subtract(atomicOrd(this.min)).add(BigInteger.ONE);
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

	private BigInteger atomicOrd(int[][] s) {
		// TODO: second int argument for order index 0..2
		int o = 0;
		BigInteger result = BigInteger.valueOf(s.length);
		for (int i = 0; i < s.length; i++)
			result = result.add(q[i].multiply(BigInteger.valueOf(s[i][o])));
		return result;
	}

	@Override
	public BigInteger cardinality() {
		return card;
	}

	@Override
	public BigInteger getOrderValue() throws CelestaException {
		int[][] arr;
		arr = toArray(value);
		// System.out.println(Arrays.toString(arr));
		// TODO: full order
		return atomicOrd(arr).subtract(minOrd);
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
			else if (c >= collator.getPrimOrderCount())
				c = collator.getPrimOrderCount() - 1;
			try {
				sb.append(collator.getElement(c, 0, 0));
			} catch (CelestaException e) {
				// do nothing
			}
			if (r.equals(BigInteger.ZERO))
				break;
		}
		this.value = sb.toString();
	}
}

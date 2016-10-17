package ru.curs.lyra.grid;

import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;

/**
 * A Collator for Lyra needs that resembles RuleBasedCollator, but not quite it
 * is.
 */
public final class LyraCollator {

	private static final HashMap<String, LyraCollator> CACHE = new HashMap<>();

	private static final int BYTEMASK = 0x000000ff;
	private static final int WORDMASK = 0x0000ffff;

	private static final int PRIMARYORDERSHIFT = 16;
	private static final int SECONDARYORDERSHIFT = 8;

	private int primOrderCount;
	private int secOrderCount;
	private int terOrderCount;

	private final TreeMap<Integer, Character> codeToElement = new TreeMap<>();
	private final HashMap<Character, Integer> elementToCode = new HashMap<>();

	private final HashSet<Character> ignoredElements = new HashSet<>();

	private final String name;

	private LyraCollator(String rules, String name) {
		parseRules(rules);
		this.name = name;
	}

	/**
	 * Gets an instance of Lyra Collator for given rules from pool or creates a
	 * new one.
	 * 
	 * @param rules
	 *            collation rules.
	 * @param name
	 *            collator name.
	 * 
	 */
	public static synchronized LyraCollator getInstance(String rules, String name) {
		LyraCollator result = CACHE.get(rules);
		if (result == null) {
			result = new LyraCollator(rules, name);
			CACHE.put(rules, result);
		}
		return result;
	}

	private static int getElementCode(int primOrder, int secOrder, int terOrder) {
		return ((primOrder & WORDMASK) << PRIMARYORDERSHIFT) | ((secOrder & BYTEMASK) << SECONDARYORDERSHIFT)
				| (terOrder & BYTEMASK);
	}

	boolean isIgnored(char c) {
		return ignoredElements.contains(c);
	}

	int getElementCode(char c) throws LyraCollatorException {
		Integer e = elementToCode.get(c);
		if (e == null)
			throw new LyraCollatorException(c);
		return e.intValue();
	}

	char getElement(int primOrder, int secOrder, int terOrder) {
		if (primOrder < 0 || primOrder >= primOrderCount)
			throw new IndexOutOfBoundsException();
		if (secOrder < 0 || secOrder >= secOrderCount)
			throw new IndexOutOfBoundsException();
		if (terOrder < 0 || terOrder >= terOrderCount)
			throw new IndexOutOfBoundsException();

		int e = getElementCode(primOrder, secOrder, terOrder);
		Character c = codeToElement.floorEntry(e).getValue();
		return c.charValue();
	}

	private void parseRules(String rules) {

		primOrderCount = 0;
		secOrderCount = 0;
		terOrderCount = 0;

		int state = 0;
		int s = 0;
		int t = 0;

		char c = ' ';

		CollatorRulesLexer lexer = new CollatorRulesLexer(rules);
		int value;
		while ((value = lexer.next()) != CollatorRulesLexer.END_OF_RULES) {
			switch (state) {
			case 0:
				switch (value) {
				case CollatorRulesLexer.CHARACTER:
					ignoredElements.add(lexer.getValue());
					break;
				case CollatorRulesLexer.PRIMARY_SEPARATOR:
					state = 1;
					break;
				default:
				}
				break;
			case 1:
				switch (value) {
				case CollatorRulesLexer.CHARACTER:
					c = lexer.getValue();
					break;
				case CollatorRulesLexer.PRIMARY_SEPARATOR:
					putChar(s, t, c);
					primOrderCount++;
					updateMaxSecOrder(s);
					updateMaxTerOrder(t);
					s = 0;
					t = 0;
					break;
				case CollatorRulesLexer.SECONDARY_SEPARATOR:
					putChar(s, t, c);
					s++;
					updateMaxTerOrder(t);
					t = 0;
					break;
				case CollatorRulesLexer.TERNARY_SEPARATOR:
					putChar(s, t, c);
					t++;
					break;
				default:
				}
				break;
			default:
			}
		}
		putChar(s, t, c);
		updateMaxTerOrder(t);
		updateMaxSecOrder(s);

		primOrderCount++;
		secOrderCount++;
		terOrderCount++;

	}

	private void putChar(int s, int t, char c) {
		int e = getElementCode(primOrderCount, s, t);
		codeToElement.put(e, c);
		elementToCode.put(c, e);
	}

	private void updateMaxSecOrder(int s) {
		if (s > secOrderCount)
			secOrderCount = s;
	}

	private void updateMaxTerOrder(int t) {
		if (t > terOrderCount)
			terOrderCount = t;
	}

	/**
	 * Maximum primary order for collator.
	 */
	public int getPrimOrderCount() {
		return primOrderCount;
	}

	/**
	 * Maximum secondary order for collator.
	 */
	public int getSecOrderCount() {
		return secOrderCount;
	}

	/**
	 * Maximum ternary order for collator.
	 */
	public int getTerOrderCount() {
		return terOrderCount;
	}

	/**
	 * Returns a LyraCollationElementIterator for the given String.
	 *
	 * @param source
	 *            the string to be collated
	 * @return a LyraCollationElementIterator object
	 *
	 */
	public LyraCollationElementIterator getCollationElementIterator(String source) {
		return new LyraCollationElementIterator(source, this);
	}

	/**
	 * Returns collator's name.
	 */
	public String getName() {
		return name;
	}

}

/**
 * Lexer for parsing the collator rules.
 *
 */
final class CollatorRulesLexer {

	public static final int END_OF_RULES = -1;
	public static final int CHARACTER = 0;
	public static final int PRIMARY_SEPARATOR = 1;
	public static final int SECONDARY_SEPARATOR = 2;
	public static final int TERNARY_SEPARATOR = 3;

	private final String rules;
	private int i = 0;
	private char c;
	private int state;

	CollatorRulesLexer(String rules) {
		this.rules = rules;
	}

	/**
	 * Next lexem in rule.
	 */
	// CHECKSTYLE:OFF for cyclomatic complexity: this is DFSM!
	public int next() {
		// CHECKSTYLE:ON
		while (i < rules.length()) {
			c = rules.charAt(i);
			i++;
			switch (state) {
			case 0:
				switch (c) {
				case '<':
					return PRIMARY_SEPARATOR;
				case ';':
					return SECONDARY_SEPARATOR;
				case ',':
					return TERNARY_SEPARATOR;
				case '\'':
					state = 1;
					break;
				case ' ':
					state = 3;
					break;
				default:
					return CHARACTER;
				}
				break;
			case 1:
				state = 2;
				break;
			case 2:
				// closing apostrophe
				if (c == '\'') {
					state = 0;
					c = rules.charAt(i - 2);
					return CHARACTER;
				}
				break;
			case 3:
				if (c != ' ') {
					state = 0;
					i--;
				}
				break;
			default:
				break;
			}
		}
		return END_OF_RULES;
	}

	/**
	 * Returns current lexer value.
	 */
	public char getValue() {
		return c;
	}

}
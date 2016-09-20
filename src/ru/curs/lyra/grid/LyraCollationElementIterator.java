package ru.curs.lyra.grid;

/**
 * This class is used as an iterator to walk through each character of an
 * international string. Use the iterator to return the ordering priority of the
 * positioned character. The ordering priority of a character, which we refer to
 * as a key, defines how a character is collated in the given collation object.
 */
public class LyraCollationElementIterator {
	private static final int PRIMARYORDERMASK = 0xffff0000;
	private static final int SECONDARYORDERMASK = 0x0000ff00;
	private static final int TERTIARYORDERMASK = 0x000000ff;

	private static final int PRIMARYORDERSHIFT = 16;
	private static final int SECONDARYORDERSHIFT = 8;

	private final String source;
	private final LyraCollator owner;
	private int i = 0;
	private int element;

	LyraCollationElementIterator(String source, LyraCollator owner) {
		this.source = source;
		this.owner = owner;
	}

	/**
	 * Move to the next collation element.
	 * 
	 * @return false if the end of the string is reached.
	 * @throws LyraCollatorException
	 *             for unknown character.
	 */
	public boolean next() throws LyraCollatorException {
		while (i < source.length()) {
			char c = source.charAt(i);
			i++;
			if (!owner.isIgnored(c)) {
				element = owner.getElementCode(c);
				return true;
			}
		}
		return false;

	}

	/**
	 * Primary order of current collation element.
	 */
	public int primaryOrder() {
		return (element & PRIMARYORDERMASK) >>> PRIMARYORDERSHIFT;
	}

	/**
	 * Secondary order of current collation element.
	 */
	public int secondaryOrder() {
		return (element & SECONDARYORDERMASK) >> SECONDARYORDERSHIFT;
	}

	/**
	 * Ternary order of current collation element.
	 */
	public int tertiaryOrder() {
		return element & TERTIARYORDERMASK;
	}
}

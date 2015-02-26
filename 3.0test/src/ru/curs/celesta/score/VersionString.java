package ru.curs.celesta.score;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Строка версии, которая обязана состоять из version-тэгов, перечисленных через
 * запятую.
 * 
 */
public final class VersionString {

	/**
	 * Строка версии по умолчанию для вновь создаваемых динамически гранул,
	 * соответствует "1.00".
	 */
	public static final VersionString DEFAULT;

	/**
	 * Результат сравнения VersionStrings, на которых существует частичный
	 * порядок (использование стандартного интерфейса Comparable невозможно, т.
	 * к. он подразумевает линейный порядок).
	 * 
	 */
	public enum ComparisionState {
		/**
		 * Текущая версия больше чем сравниваемая.
		 */
		GREATER, /**
		 * Текущая версия равна сравниваемой.
		 */
		EQUALS, /**
		 * Текущая версия меньше сравниваемой.
		 */
		LOWER, /**
		 * Текущие версии несравнимы.
		 */
		INCONSISTENT
	}

	private static final Pattern P = Pattern
			.compile("([A-Z_]*)([0-9]+\\.[0-9]+)");

	static {
		VersionString v;
		try {
			v = new VersionString("1.00");
		} catch (ParseException e) {
			v = null;
		}
		DEFAULT = v;
	}

	private final Map<String, Double> versions = new HashMap<>();
	private final String versionString;
	private final int hashCode;

	public VersionString(String versionString) throws ParseException {
		if (versionString == null)
			throw new IllegalArgumentException();
		if ("".equals(versionString))
			throw new ParseException("Empty grain version string.");
		this.versionString = versionString;
		int h = 0;
		for (String version : versionString.split(",")) {
			Matcher m = P.matcher(version);
			if (m.matches()) {
				versions.put(m.group(1), Double.parseDouble(m.group(2)));
			} else {
				throw new ParseException(
						String.format(
								"Invalid grain version string: version component '%s' does not matches pattern '%s'",
								version, P.toString()));
			}
			// От перестановки местами version-tag-ов сумма хэшкода не меняется.
			h ^= version.hashCode();
		}
		hashCode = h;
	}

	private int compareValues(Double v1, Double v2) {

		if (v1 == null && v2 == null)
			throw new IllegalArgumentException();

		if (v1 != null) {
			if (v2 == null || v2 < v1) {
				return 1;
			} else if (v2 > v1) {
				return -1;
			} else {
				return 0;
			}
		}

		return -1;
	}

	/**
	 * Сравнение с учётом существования частичного порядка на версиях.
	 * 
	 * @param o
	 *            объект, с которым сравнивается текущая версия.
	 * 
	 */
	public ComparisionState compareTo(VersionString o) {
		if (o == null)
			throw new IllegalArgumentException();

		Set<String> tags = new HashSet<>();
		tags.addAll(versions.keySet());
		tags.addAll(o.versions.keySet());

		ComparisionState result = ComparisionState.EQUALS;

		for (String tag : tags) {

			int compare = compareValues(versions.get(tag), o.versions.get(tag));
			switch (result) {
			case EQUALS:
				if (compare > 0)
					result = ComparisionState.GREATER;
				else if (compare < 0)
					result = ComparisionState.LOWER;
				break;
			case GREATER:
				if (compare < 0)
					result = ComparisionState.INCONSISTENT;
				break;
			case LOWER:
				if (compare > 0)
					result = ComparisionState.INCONSISTENT;
				break;
			default:
			}
			if (result == ComparisionState.INCONSISTENT)
				break;
		}
		return result;
	}

	@Override
	public int hashCode() {
		return hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof VersionString) {
			return compareTo((VersionString) obj) == ComparisionState.EQUALS;
		} else {
			return super.equals(obj);
		}
	}

	@Override
	public String toString() {
		return versionString;
	}

}

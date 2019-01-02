package ru.curs.celesta.score;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Version string that has to consist of comma separated version tags.
 */
public final class VersionString {

    /**
     * Default version string for newly created dynamic grains.
     * Corresponds to "1.00".
     */
    public static final VersionString DEFAULT;

    /**
     * The result of comparison of VersionStrings on which a partial order is
     * defined. (the use of standard interface Comparable is impossible, for it
     * implies a linear order).
     *
     */
    public enum ComparisionState {
        /**
         * Current version is greater than the compared one.
         */
        GREATER,
        /**
         * Current version equals to the compared one.
         */
        EQUALS,
        /**
         * Current version is lesser than the compared one.
         */
        LOWER,
        /**
         * Current versions are incomparable. 
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
        if (versionString == null) {
            throw new IllegalArgumentException();
        }
        if ("".equals(versionString)) {
            throw new ParseException("Empty grain version string.");
        }
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

        if (v1 == null && v2 == null) {
            throw new IllegalArgumentException();
        }

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
     * Comparison based on the existence of partial order on versions.
     *
     * @param o  object that current version is being compared with.
     *
     */
    public ComparisionState compareTo(VersionString o) {
        if (o == null) {
            throw new IllegalArgumentException();
        }

        Set<String> tags = new HashSet<>();
        tags.addAll(versions.keySet());
        tags.addAll(o.versions.keySet());

        ComparisionState result = ComparisionState.EQUALS;

        for (String tag : tags) {

            int compare = compareValues(versions.get(tag), o.versions.get(tag));
            switch (result) {
            case EQUALS:
                if (compare > 0) {
                    result = ComparisionState.GREATER;
                } else if (compare < 0) {
                    result = ComparisionState.LOWER;
                }
                break;
            case GREATER:
                if (compare < 0) {
                    result = ComparisionState.INCONSISTENT;
                }
                break;
            case LOWER:
                if (compare > 0) {
                    result = ComparisionState.INCONSISTENT;
                }
                break;
            default:
            }
            if (result == ComparisionState.INCONSISTENT) {
                break;
            }
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

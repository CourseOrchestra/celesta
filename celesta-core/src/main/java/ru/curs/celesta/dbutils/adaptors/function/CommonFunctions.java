package ru.curs.celesta.dbutils.adaptors.function;

/**
 * Utility class for common functions.
 */
public final class CommonFunctions {

    private CommonFunctions() {
        throw new AssertionError();
    }

    /**
     * Adds ", " {@link CharSequence} to input {@link StringBuilder} if it's not empty.
     * @param insertList {@link StringBuilder} to process.
     */
    public static void padComma(StringBuilder insertList) {
        if (insertList.length() > 0) {
            insertList.append(", ");
        }
    }

    /**
     * Transforms {@link Iterable} into comma separated {@link String} values.
     * @param fields {@link Iterable} values to transform.
     * @return Comma separated {@link String} values.
     */
    public static String getFieldList(Iterable<String> fields) {
        StringBuilder sb = new StringBuilder();
        for (String c : fields) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append('"');
            sb.append(c);
            sb.append('"');
        }
        return sb.toString();
    }

}

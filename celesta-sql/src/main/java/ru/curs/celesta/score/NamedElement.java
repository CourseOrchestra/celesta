package ru.curs.celesta.score;

import ru.curs.celesta.score.validator.IdentifierParser;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A named element of metamodel (e.g. table or column) that must have
 * a unique identifier name.
 */
public abstract class NamedElement {

    /**
     * The maximal length of an identifier of Celesta.
     */
    public static final int MAX_IDENTIFIER_LENGTH = 30;

    private static final Pattern COMMENT = Pattern.compile("/\\*\\*(.*)\\*/", Pattern.DOTALL);

    private final String name;
    private final String quotedName;

    private String celestaDoc;

    public NamedElement(String name, IdentifierParser identifierParser) throws ParseException {
        // name==null should not happen, for all methods are written assuming
        // that name != null.
        if (name == null) {
            throw new IllegalArgumentException();
        }
        this.name = identifierParser.parse(name);
        this.quotedName = String.format("\"%s\"", this.name);
    }

    /**
     * Restricts identifier length by maximal number of symbols.
     *
     * @param value  Identifier of arbitrary length.
     * @return "Shortcut" identifier that has a hash code of the original one as its
     *         last 8 symbols.
     */
    public static String limitName(String value) {
        return limitName(value, "");
    }

    /**
     * Restricts identifier length with a postfix by maximal number of symbols. The resulting form
     * becomes: &lt;restricted identifier&gt;&lt;postfix&gt;.
     * For example <em>my_very_long_table_name</em> with <em>_nextValueProc</em> becomes -
     * <em>my_very_73FAF9A9_nextValueProc</em>
     *
     * @param value  Identifier of arbitrary length.
     * @param postfix  Identifier postfix.
     *
     * @return "Shortcut" identifier that has a hash code of the original one as its
     *         last 8 symbols plus postfix.
     */
    public static String limitName(String value, String postfix) {
        String result = value;
        if (result.length() + postfix.length() > NamedElement.MAX_IDENTIFIER_LENGTH) {
            int trimLength = NamedElement.MAX_IDENTIFIER_LENGTH - postfix.length() - 8;
            if (trimLength < 4) {
                throw new IllegalArgumentException(
                        String.format("Restricted name for %s couldn't be created.", value + postfix));
            }
            result = String.format("%s%08X", result.substring(0, trimLength), result.hashCode());
        }

        return result + postfix;
    }

    /**
     * Returns the name.
     */
    public final String getName() {
        return name;
    }

    /**
     * Returns the name in ANSI quotes (e.g. <b>"celestaIdentifier"</b>).
     */
    public final String getQuotedName() {
        return quotedName;
    }

    /**
     * Returns the name in ANSI quotes if needed.
     * @return
     */
    public final String getQuotedNameIfNeeded() {
        Pattern p = Pattern.compile(IdentifierParser.PLAIN_NAME_PATTERN_STR);
        if (p.matcher(name).matches()) {
            return name;
        } else {
            return quotedName;
        }
    }

    @Override
    public final int hashCode() {
        return name.hashCode();
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof NamedElement) {
            return Objects.equals(this.name, ((NamedElement) obj).getName());
        }

        return false;
    }

    /**
     * Returns value of document string for this element.
     *
     * @return
     */
    public String getCelestaDoc() {
        return celestaDoc;
    }

    /**
     * Sets value of document string in commented form.
     *
     * @param celestaDoc  new value
     * @throws ParseException  if the comment has a wrong format
     */
    void setCelestaDocLexem(String celestaDoc) throws ParseException {
        if (celestaDoc == null) {
            this.celestaDoc = null;
        } else {
            Matcher m = COMMENT.matcher(celestaDoc);
            if (!m.matches()) {
                throw new ParseException("Celestadoc should match pattern /**...*/, was " + celestaDoc);
            }
            setCelestaDoc(m.group(1));
        }
    }

    /**
     * Sets value of document string.
     *
     * @param celestaDoc  new value
     * @throws ParseException  incorrect CelestaDoc
     */
    public void setCelestaDoc(String celestaDoc) throws ParseException {
        this.celestaDoc = celestaDoc;
    }

}

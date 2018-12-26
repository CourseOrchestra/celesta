package ru.curs.celesta.score;

import ru.curs.celesta.score.validator.IdentifierParser;

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
        String result = value;
        if (result.length() > NamedElement.MAX_IDENTIFIER_LENGTH) {
            result = String.format("%s%08X", result.substring(0, NamedElement.MAX_IDENTIFIER_LENGTH - 8),
                    result.hashCode());
        }
        return result;
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
        return obj instanceof NamedElement ? name.equals(((NamedElement) obj).getName()) : name.equals(obj);
    }

    /**
     * Возвращает значение документационной строки для данного элемента.
     */
    public String getCelestaDoc() {
        return celestaDoc;
    }

    /**
     * Устанавливает значение документационной строки в закомментированном виде.
     *
     * @param celestaDoc
     *            новое значение.
     * @throws ParseException
     *             Если комментарий имеет неверный формат.
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
     * Устанавливает значение документационной строки.
     *
     * @param celestaDoc
     *            новое значение.
     * @throws ParseException
     *            неверный CelestaDoc.
     */
    public void setCelestaDoc(String celestaDoc) throws ParseException {
        this.celestaDoc = celestaDoc;
    }
}

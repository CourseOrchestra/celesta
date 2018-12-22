package ru.curs.celesta.score;

import ru.curs.celesta.score.validator.IdentifierParser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Именованный элемент метамодели (например, таблица или колонка), который
 * должен иметь уникальное имя-идентификатор.
 *
 */
public abstract class NamedElement {

    /**
     * Максимальная длина идентификатора Celesta.
     */
    public static final int MAX_IDENTIFIER_LENGTH = 30;

    private static final Pattern COMMENT = Pattern.compile("/\\*\\*(.*)\\*/", Pattern.DOTALL);

    private final String name;
    private final String quotedName;

    private String celestaDoc;

    public NamedElement(String name, IdentifierParser identifierParser) throws ParseException {
        // Не должно быть name==null, т. к. все методы написаны исходя из того,
        // что name != null.
        if (name == null) {
            throw new IllegalArgumentException();
        }
        this.name = identifierParser.parse(name);
        this.quotedName = String.format("\"%s\"", this.name);
    }

    /**
     * Ограничивает длину идентификатора максимальным числом символов.
     *
     * @param value
     *            Идентификатор произвольной длины.
     * @return "Подрезанный" идентификатор, последние 8 символов занимает
     *         хэш-код исходного идентификатора.
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
     * Возвращает имя.
     */
    public final String getName() {
        return name;
    }

    /**
     * Возвращает имя в прямых кавычках ("ANSI quotes").
     */
    public final String getQuotedName() {
        return quotedName;
    }

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

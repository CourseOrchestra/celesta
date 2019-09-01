package ru.curs.celesta.score;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * View column metadata.
 *
 * @param <V>  Java class of column value
 */
public final class ViewColumnMeta<V> implements ColumnMeta<V> {

    private static final Pattern COMMENT = Pattern.compile("/\\*\\*(.*)\\*/", Pattern.DOTALL);

    private final ViewColumnType type;
    private String name;
    private String celestaDoc = "";
    private boolean nullable = true;
    private final int length;

    public ViewColumnMeta(ViewColumnType type) {
        if (type == null) {
            throw new IllegalArgumentException();
        }
        this.type = type;
        this.length = -1;
    }

    public ViewColumnMeta(ViewColumnType type, int length) {
        if (type == null) {
            throw new IllegalArgumentException();
        }
        this.type = type;
        this.length = length;
    }

    /**
     * Returns column type.
     *
     * @return
     */
    public ViewColumnType getColumnType() {
        return type;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String jdbcGetterName() {
        return type.jdbcGetterName();
    }

    @Override
    public String getCelestaType() {
        return type.getCelestaType();
    }

    @Override
    public Class<?> getJavaClass() {
        return type.getJavaClass();
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }

    @Override
    public String getCelestaDoc() {
        return celestaDoc;
    }

    /**
     * Sets column name.
     *
     * @param name
     *            new value
     */
    void setName(String name) {
        this.name = name;
    }

    /**
     * Sets 'nullable' flag.
     *
     * @param nullable
     *            new value.
     */
    void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    /**
     * Sets CelestaDoc lexem.
     *
     * @param celestaDoc
     *            new value.
     * @throws ParseException
     *             wrong CelestaDoc.
     */
    void setCelestaDocLexem(String celestaDoc) throws ParseException {
        if (celestaDoc == null) {
            this.celestaDoc = null;
        } else {
            Matcher m = COMMENT.matcher(celestaDoc);
            if (!m.matches()) {
                throw new ParseException("Celestadoc should match pattern /**...*/, was " + celestaDoc);
            }
            this.celestaDoc = m.group(1);
        }
    }

    /**
     * Sets CelestaDoc lexem.
     *
     * @param celestaDoc
     *            new value.
     */
    void setCelestaDoc(String celestaDoc) {
        this.celestaDoc = celestaDoc;
    }

    /**
     * Returns field's length (or -1 if undefined).
     *
     * @return
     */
    public int getLength() {
        return length;
    }

}

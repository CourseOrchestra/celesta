package ru.curs.celesta.score;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ru.curs.celesta.CelestaException;

/**
 * Integer column.
 */
public final class IntegerColumn extends Column {
    /**
     * Celesta type of the column data.
     */
    public static final String CELESTA_TYPE = "INT";
    private Integer defaultvalue;
    private SequenceElement sequence;

    public IntegerColumn(TableElement table, String name) throws ParseException {
        super(table, name);
    }

    IntegerColumn(TableElement table) throws ParseException {
        super(table);
    }

    @Override
    protected void setDefault(String lexvalue) throws ParseException {
        if (lexvalue == null) {
            defaultvalue = null;
            sequence = null;
        } else {
            Pattern p = Pattern.compile("(?i)NEXTVAL\\((.*)\\)");
            Matcher m = p.matcher(lexvalue);
            if (m.matches()) {
                defaultvalue = null;
                String sequenceName = getParentTable().getGrain().getScore().getIdentifierParser().parse(m.group(1));
                sequence = getParentTable().getGrain().getElement(sequenceName, SequenceElement.class);
            } else {
                defaultvalue = Integer.parseInt(lexvalue);
                sequence = null;
            }
        }
    }

    @Override
    public Integer getDefaultValue() {
        return defaultvalue;
    }

    /**
     * Returns sequence of the column. 
     * @return
     */
    public SequenceElement getSequence() {
        return sequence;
    }

    @Override
    public String jdbcGetterName() {
        return "getInt";
    }

    @Override
    public String getCelestaType() {
        return CELESTA_TYPE;
    }

    @Override
    public Class<?> getJavaClass() {
        return Integer.class;
    }

    @Override
    public String getCelestaDefault() {
        return defaultvalue == null ? null : defaultvalue.toString();
    }

    @Override
    public void setCelestaDoc(String celestaDoc) throws ParseException {
        super.setCelestaDoc(celestaDoc);
        // check options validity
        try {
            getOptions();
        } catch (CelestaException e) {
            throw new ParseException(e.getMessage());
        }
    }

}

package ru.curs.celesta.score;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ru.curs.celesta.CelestaException;

/**
 * Целочисленная колонка.
 * 
 */
public final class IntegerColumn extends Column {
	/**
	 * Celesta-тип данных колонки.
	 */
	public static final String CELESTA_TYPE = "INT";
	private Integer defaultvalue;
	private boolean identity;
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
			identity = false;
			sequence = null;
		} else {
			Pattern p = Pattern.compile("(?i)NEXTVAL\\((.*)\\)");
			Matcher m = p.matcher(lexvalue);
			if (m.matches()) {
				defaultvalue = null;
				identity = false;
				String sequenceName = m.group(1);
				sequence = getParentTable().getGrain().getElement(sequenceName, SequenceElement.class);
			} else if ("IDENTITY".equalsIgnoreCase(lexvalue)) {
				for (Column c : getParentTable().getColumns().values())
					if (c instanceof IntegerColumn && c != this && ((IntegerColumn) c).isIdentity())
						throw new ParseException(
								"More than one identity columns are defined in table " + getParentTable().getName());
				defaultvalue = null;
				identity = true;
				sequence = null;
			} else {
				defaultvalue = Integer.parseInt(lexvalue);
				identity = false;
				sequence = null;
			}
		}
	}

	@Override
	public Integer getDefaultValue() {
		return defaultvalue;
	}

	public SequenceElement getSequence() {
		return sequence;
	}

	/**
	 * Является ли поле IDENTITY.
	 */
	public boolean isIdentity() {
		return identity;
	}

	@Override
	public String jdbcGetterName() {
		return "getInt";
	}

	@Override
	void save(PrintWriter bw) throws IOException {
		super.save(bw);
		bw.write(" INT");
		if (!isNullable())
			bw.write(" NOT NULL");
		Integer defaultVal = getDefaultValue();
		if (defaultVal != null) {
			bw.write(" DEFAULT ");
			bw.write(defaultVal.toString());
		}
		if (identity)
			bw.write(" IDENTITY");
	}

	@Override
	public String getCelestaType() {
		return CELESTA_TYPE;
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

package ru.curs.celesta;

public class IntegerColumn extends Column {
	public IntegerColumn(String name) {
		super(name);
	}

	private Integer defaultvalue;
	private boolean identity;

	@Override
	protected void setDefault(String lexvalue) {
		if (lexvalue == null) {
			defaultvalue = null;
			identity = false;
		} else if ("IDENTITY".equalsIgnoreCase(lexvalue)) {
			defaultvalue = null;
			identity = true;
		} else {
			defaultvalue = Integer.parseInt(lexvalue);
			identity = false;
		}
	}

	public Integer getDefaultvalue() {
		return defaultvalue;
	}

	public boolean isIdentity() {
		return identity;
	}
}

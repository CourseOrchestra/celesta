package ru.curs.celesta;

public abstract class Column extends NamedElement {

	Column(String name) {
		super(name);
	}

	protected abstract void setDefault(String lexvalue) throws ParseException;

	private boolean nullable = true;

	@Override
	public String toString() {
		return getName();
	}

	public void setNullableAndDefault(boolean nullable, String defaultValue)
			throws ParseException {
		if (defaultValue == null && !nullable)
			throw new ParseException(
					String.format(
							"Column %s is defined as NOT NULL but has no default value",
							getName()));
		this.nullable = nullable;
		setDefault(defaultValue);
	}

	final boolean isNullable() {
		return nullable;
	}
}

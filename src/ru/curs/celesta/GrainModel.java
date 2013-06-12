package ru.curs.celesta;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public class GrainModel {
	private final Set<Table> tables = new LinkedHashSet<>();

	public void addTable(Table table) throws ParseException {
		if (!tables.add(table))
			throw new ParseException(String.format(
					"Table '%s' defined more than once in a grain.",
					table.getName()));

	}

	public Set<Table> getTables() {
		return Collections.unmodifiableSet(tables);
	}

	@Override
	public String toString() {
		return tables.toString();
	}

}

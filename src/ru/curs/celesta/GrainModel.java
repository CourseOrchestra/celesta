package ru.curs.celesta;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class GrainModel {
	private final Map<String, Table> tables = new LinkedHashMap<>();

	public void addTable(Table table) throws ParseException {
		if (tables.put(table.getName(), table) != null)
			throw new ParseException(String.format(
					"Table '%s' defined more than once in a grain.",
					table.getName()));

	}

	public Map<String, Table> getTables() {
		return Collections.unmodifiableMap(tables);
	}

	@Override
	public String toString() {
		return tables.toString();
	}

}

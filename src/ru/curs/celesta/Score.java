package ru.curs.celesta;

import java.util.Map;

/**
 * Корневой класс полной модели данных гранул.
 * 
 */
public class Score {
	private final NamedElementHolder<Grain> grains = new NamedElementHolder<Grain>() {
		@Override
		String getErrorMsg(String name) {
			return String.format(
					"Grain '%s' defined more than once in a score.", name);
		}
	};

	void addGrain(Grain grain) throws ParseException {
		if (grain.getScore() != this)
			throw new IllegalArgumentException();
		grains.addElement(grain);
	}

	public Map<String, Grain> getGrains() {
		return grains.getElements();
	}
}

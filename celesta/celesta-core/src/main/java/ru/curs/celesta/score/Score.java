/*
   (с) 2013 ООО "КУРС-ИТ"  

   Этот файл — часть КУРС:Celesta.
   
   КУРС:Celesta — свободная программа: вы можете перераспространять ее и/или изменять
   ее на условиях Стандартной общественной лицензии GNU в том виде, в каком
   она была опубликована Фондом свободного программного обеспечения; либо
   версии 3 лицензии, либо (по вашему выбору) любой более поздней версии.

   Эта программа распространяется в надежде, что она будет полезной,
   но БЕЗО ВСЯКИХ ГАРАНТИЙ; даже без неявной гарантии ТОВАРНОГО ВИДА
   или ПРИГОДНОСТИ ДЛЯ ОПРЕДЕЛЕННЫХ ЦЕЛЕЙ. Подробнее см. в Стандартной
   общественной лицензии GNU.

   Вы должны были получить копию Стандартной общественной лицензии GNU
   вместе с этой программой. Если это не так, см. http://www.gnu.org/licenses/.

   
   Copyright 2013, COURSE-IT Ltd.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   You should have received a copy of the GNU General Public License
   along with this program.  If not, see http://www.gnu.org/licenses/.

 */
package ru.curs.celesta.score;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;

import ru.curs.celesta.CelestaException;

/**
 * Корневой класс полной модели данных гранул.
 * 
 */
public class Score {

	private final Map<String, Grain> grains = new HashMap<>();

	private final Map<String, File> grainFiles = new HashMap<>();

	private String path;
	private File defaultGrainPath;

	Score() {

	}

	/**
	 * Инициализация ядра путём указания набора путей к папкам score,
	 * разделённого точкой с запятой.
	 * 
	 * @param scorePath
	 *            набор путей к папкам score, разделённый точкой с запятой.
	 * @throws CelestaException
	 *             в случае указания несуществующего пути или в случае двойного
	 *             определения гранулы с одним и тем же именем.
	 */
	public Score(String scorePath) throws CelestaException {
		this.path = scorePath;
		for (String entry : scorePath.split(File.pathSeparator)) {
			File path = new File(entry.trim());
			if (!path.exists())
				throw new CelestaException("Score path entry '%s' does not exist.", path.toString());
			if (!path.canRead())
				throw new CelestaException("Cannot read score path entry '%s'.", path.toString());
			if (!path.isDirectory())
				throw new CelestaException("Score path entry '%s' is not a directory.", path.toString());

			defaultGrainPath = path;

			for (File grainPath : path.listFiles(new FileFilter() {
				@Override
				public boolean accept(File pathname) {
					return pathname.isDirectory();
				}
			})) {
				String grainName = grainPath.getName();
				File scriptFile = new File(
						String.format("%s%s_%s.sql", grainPath.getPath(), File.separator, grainName));

				if (scriptFile.exists()) {
					/*
					 * Наличие sql-файла говорит о том, что мы имеем дело с
					 * папкой гранулы, и уже требуем от неё всё подряд.
					 */
					File initFile = new File(String.format("%s%s__init__.py", grainPath.getPath(), File.separator));
					if (!initFile.exists())
						throw new CelestaException("Cannot find __init__.py in grain '%s' definition folder.",
								grainName);

					if (!scriptFile.canRead())
						throw new CelestaException("Cannot read script file '%s'.", scriptFile);
					if (grainFiles.containsKey(grainName))
						throw new CelestaException("Grain '%s' defined more than once on different paths.", grainName);
					grainFiles.put(grainName, scriptFile);
				}

			}
		}

		initSystemGrain();
		// В этот момент в таблице grainFiles содержится перечень распознанных
		// имён гранул с именами файлов-скриптов.
		parseGrains();
	}

	/**
	 * Сохраняет содержимое метаданных обратно в SQL-файлы, при этом
	 * перезаписывая их содержимое.
	 * 
	 * @throws CelestaException
	 *             при ошибке ввода-вывода.
	 */
	public void save() throws CelestaException {
		for (Grain g : grains.values())
			if (g.isModified())
				g.save();
	}

	private void parseGrains() throws CelestaException {
		StringBuilder errorScript = new StringBuilder();
		for (String s : grainFiles.keySet())
			try {
				getGrain(s);
			} catch (ParseException e) {
				if (errorScript.length() > 0)
					errorScript.append("\n\n");
				errorScript.append(e.getMessage());
			}
		if (errorScript.length() > 0)
			throw new CelestaException(errorScript.toString());
	}

	void addGrain(Grain grain) throws ParseException {
		if (grain.getScore() != this)
			throw new IllegalArgumentException();
		if (grains.containsKey(grain.getName()))
			throw new ParseException(String.format("Grain '%s' is already defined.", grain.getName()));
		grains.put(grain.getName(), grain);
	}

	/**
	 * Получение гранулы по её имени. При этом, если гранула ещё не была
	 * подгружена из скрипта, производится её подгрузка. В случае, если имя
	 * гранулы неизвестно, выводится исключение.
	 * 
	 * @param name
	 *            Имя гранулы.
	 * 
	 * @throws ParseException
	 *             Если имя гранулы неизвестно системе.
	 */
	public Grain getGrain(String name) throws ParseException {
		Grain result = grains.get(name);
		if (result == null) {
			File f = grainFiles.get(name);
			if (f == null)
				throw new ParseException(String.format("Unknown grain '%s'.", name));
			ChecksumInputStream is = null;

			try {
				is = new ChecksumInputStream(new FileInputStream(f));
			} catch (FileNotFoundException e) {
				throw new ParseException(String.format("Cannot open file '%s'.", f.toString()));
			}

			CelestaParser parser = new CelestaParser(is, "utf-8");
			try {
				try {
					result = parser.grain(this, name);
				} catch (ParseException | TokenMgrError e) {
					throw new ParseException(String.format("Error parsing '%s': %s", f.toString(), e.getMessage()));
				}
				result.setChecksum(is.getCRC32());
				result.setLength(is.getCount());
				result.setGrainPath(f.getParentFile());
			} finally {
				try {
					is.close();
				} catch (IOException e) {
					// This should never happen, however.
					is = null;
				}
			}
		}

		return result;
	}

	private void initSystemGrain() throws CelestaException {
		ChecksumInputStream is = new ChecksumInputStream(Score.class.getResourceAsStream("celesta.sql"));

		CelestaParser parser = new CelestaParser(is, "utf-8");
		try {
			Grain result;
			try {
				result = parser.grain(this, "celesta");
			} catch (ParseException e) {
				throw new CelestaException(e.getMessage());
			}
			result.setChecksum(is.getCRC32());
			result.setLength(is.getCount());
		} finally {
			try {
				is.close();
			} catch (IOException e) {
				// This should never happen, however.
				is = null;
			}
		}

	}

	/**
	 * Возвращает неизменяемый набор гранул.
	 */
	public Map<String, Grain> getGrains() {
		return Collections.unmodifiableMap(grains);
	}

	/**
	 * Возвращает путь по умолчанию для создаваемых динамически гранул. Значение
	 * равно последней записи в score.path.
	 */
	File getDefaultGrainPath() {
		return defaultGrainPath;
	}


	public String getPath() {
		return path;
	}
}

/**
 * Обёртка InputStream для подсчёта контрольной суммы при чтении.
 *
 */
final class ChecksumInputStream extends InputStream {
	private final CRC32 checksum = new CRC32();
	private final InputStream input;
	private int counter = 0;

	ChecksumInputStream(InputStream input) {
		this.input = input;
	}

	@Override
	public int read() throws IOException {
		int result = input.read();
		if (result >= 0) {
			counter++;
			checksum.update(result);
		}
		return result;
	}

	public int getCRC32() {
		return (int) checksum.getValue();
	}

	public int getCount() {
		return counter;
	}

	@Override
	public void close() throws IOException {
		input.close();
	}

}
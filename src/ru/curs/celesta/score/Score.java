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
		for (String entry : scorePath.split(";")) {
			File path = new File(entry);
			if (!path.exists())
				throw new CelestaException(
						"Score path entry '%s' does not exist.",
						path.toString());
			if (!path.canRead())
				throw new CelestaException(
						"Cannot read score path entry '%s'.", path.toString());
			if (!path.isDirectory())
				throw new CelestaException(
						"Score path entry '%s' is not a directory.",
						path.toString());

			for (File grainPath : path.listFiles(new FileFilter() {
				@Override
				public boolean accept(File pathname) {
					return pathname.isDirectory();
				}
			})) {
				String grainName = grainPath.getName();
				File scriptFile = new File(String.format("%s%s_%s.sql",
						grainPath.getPath(), File.separator, grainName));

				if (scriptFile.exists()) {
					/*
					 * Наличие sql-файла говорит о том, что мы имеем дело с
					 * папкой гранулы, и уже требуем от неё всё подряд.
					 */
					File initFile = new File(String.format("%s%s__init__.py",
							grainPath.getPath(), File.separator));
					if (!initFile.exists())
						throw new CelestaException(
								"Cannot find __init__.py in grain '%s' definition folder.",
								grainName);

					if (!scriptFile.canRead())
						throw new CelestaException(
								"Cannot read script file '%s'.", scriptFile);
					if (grainFiles.containsKey(grainName))
						throw new CelestaException(
								"Grain '%s' defined more than once on different paths.",
								grainName);
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
	 */
	public void save() {
		// TODO
		throw new IllegalStateException(
				"Score saving to files not implemented yet");
	}

	private void parseGrains() throws CelestaException {
		StringBuilder errorScript = new StringBuilder();
		for (String s : grainFiles.keySet())
			try {
				getGrain(s);
			} catch (ParseException e) {
				if (errorScript.length() > 0)
					errorScript.append("\n\n");
				errorScript.append(String.format("Error parsing '%s': ",
						grainFiles.get(s)));
				errorScript.append(e.getMessage());
			}
		if (errorScript.length() > 0)
			throw new CelestaException(errorScript.toString());
	}

	void addGrain(Grain grain) throws ParseException {
		if (grain.getScore() != this)
			throw new IllegalArgumentException();
		if (grains.containsKey(grain.getName()))
			throw new ParseException(String.format(
					"Grain '%s' is already defined.", grain.getName()));
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
				throw new ParseException(String.format("Unknown grain '%s'.",
						name));
			ChecksumInputStream is = null;

			try {
				is = new ChecksumInputStream(new FileInputStream(f));
			} catch (FileNotFoundException e) {
				throw new ParseException(String.format(
						"Cannot open file '%s'.", f.toString()));
			}

			CelestaParser parser = new CelestaParser(is, "utf-8");
			try {
				result = parser.grain(this, name);
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
		ChecksumInputStream is = new ChecksumInputStream(
				Score.class.getResourceAsStream("celesta.sql"));

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
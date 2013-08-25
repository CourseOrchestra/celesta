package ru.curs.celesta.ormcompiler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.Score;

/**
 * Комилятор ORM-кода.
 */
public final class ORMCompiler {

	private ORMCompiler() {

	}

	/**
	 * Выполняет компиляцию кода на основе разобранной объектной модели.
	 * 
	 * @param score
	 *            модель
	 * @throws CelestaException
	 *             при неудаче компиляции, например, при ошибке вывода в файл.
	 */
	public static void compile(Score score) throws CelestaException {
		for (Grain g : score.getGrains().values())
			// Пропускаем системную гранулу.
			if (!"celesta".equals(g.getName())) {
				File ormFile = new File(String.format("%s%s_%s_orm.py",
						g.getGrainPath(), File.separator, g.getName()));
				try {
					FileOutputStream fos = new FileOutputStream(ormFile);
					BufferedWriter w = new BufferedWriter(
							new OutputStreamWriter(fos, "utf-8"));
					try {
						compileGrain(g, w);
					} finally {
						w.flush();
						fos.close();
					}
				} catch (IOException e) {
					throw new CelestaException(
							"Error while compiling orm classes for '%s' grain: %s",
							g.getName(), e.getMessage());
				}
			}
	}

	private static void compileGrain(Grain g, BufferedWriter w)
			throws IOException {
		w.write("# coding=UTF-8");
		w.newLine();
	}
}

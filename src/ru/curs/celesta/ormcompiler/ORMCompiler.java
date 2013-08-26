package ru.curs.celesta.ormcompiler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.Table;

/**
 * Комилятор ORM-кода.
 */
public final class ORMCompiler {

	private static final String[] HEADER = {
			"# coding=UTF-8",
			"\"\"\"",
			"THIS MODULE IS BEING CREATED AUTOMATICALLY EVERY TIME CELESTA STARTS.",
			"DO NOT MODIFY IT AS YOUR CHANGES WILL BE LOST.", "\"\"\"",
			"import ru.curs.celesta.dbutils.Cursor as Cursor", "" };

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
		for (String s : HEADER) {
			w.write(s);
			w.newLine();
		}

		for (Table t : g.getTables().values())
			compileTable(t, w);
	}

	private static void compileTable(Table t, BufferedWriter w)
			throws IOException {

		Collection<Column> columns = t.getColumns().values();
		Set<Column> pk = new LinkedHashSet<>(t.getPrimaryKey().values());

		w.write(String.format("class %sCursor(Cursor):", t.getName()));
		w.newLine();
		// Конструктор
		w.write("    def __init__(self, conn):");
		w.newLine();
		w.write("        AbstractCursor.__init(self, conn)");
		w.newLine();
		for (Column c : columns) {
			w.write(String.format("        self.%s = %s", c.getName(),
					c.pythonDefaultValue()));
			w.newLine();
		}
		// Имя гранулы
		w.write("    def grainName(self):");
		w.newLine();
		w.write(String.format("        return '%s'", t.getGrain().getName()));
		w.newLine();
		// Имя таблицы
		w.write("    def tableName(self):");
		w.newLine();
		w.write(String.format("        return '%s'", t.getName()));
		w.newLine();
		// Разбор строки по переменным
		w.write("    def parseResult(self, rs):");
		w.newLine();
		int i = 1;
		for (Column c : columns) {
			w.write(String.format("        self.%s = rs.%s(%d)", c.getName(),
					c.jdbcGetterName(), i));
			w.newLine();
			i++;
		}
		// Очистка буфера
		w.write("    def clearBuffer(self, withKeys):");
		w.newLine();
		w.write("        if withKeys:");
		w.newLine();
		for (Column c : pk) {
			w.write(String.format("            self.%s = %s", c.getName(),
					c.pythonDefaultValue()));
			w.newLine();
		}
		for (Column c : columns)
			if (!pk.contains(c)) {
				w.write(String.format("        self.%s = %s", c.getName(),
						c.pythonDefaultValue()));
				w.newLine();
			}
		// Текущие значения ключевых полей
		w.write("    def currentKeyValues(self):");
		w.newLine();
		StringBuilder sb = new StringBuilder();
		for (Column c : pk) {
			if (sb.length() > 0)
				sb.append(", ");
			sb.append(c.getName());
		}
		w.write(String.format("        return [%s]", sb.toString()));
		w.newLine();
		// Текущие значения всех полей
		w.write("    def currentValues(self):");
		w.newLine();
		sb = new StringBuilder();
		for (Column c : columns) {
			if (sb.length() > 0)
				sb.append(", ");
			sb.append(c.getName());
		}
		w.write(String.format("        return [%s]", sb.toString()));
		w.newLine();
		w.newLine();
	}
}

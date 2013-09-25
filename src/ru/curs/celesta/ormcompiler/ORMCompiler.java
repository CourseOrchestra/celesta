package ru.curs.celesta.ormcompiler;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.Table;

/**
 * Комилятор ORM-кода.
 */
public final class ORMCompiler {

	private static final Pattern SIGNATURE = Pattern
			.compile("len=([0-9]+), crc32=([0-9A-F]+)\\.");
	private static final String[] HEADER = {
			"\"\"\"",
			"THIS MODULE IS BEING CREATED AUTOMATICALLY EVERY TIME CELESTA STARTS.",
			"DO NOT MODIFY IT AS YOUR CHANGES WILL BE LOST.", "\"\"\"",
			"import ru.curs.celesta.dbutils.Cursor as Cursor",
			"from java.lang import Object", "from jarray import array", "" };

	private static final String[] TABLE_HEADER = { "    onPreDelete  = []",
			"    onPostDelete = []", "    onPreInsert  = []",
			"    onPostInsert = []", "    onPreUpdate  = []",
			"    onPostUpdate = []" };
	private static final String F_SELF = "            f(self)";

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
					// Блок проверки: а может, перекомпилировать нет нужды?
					if (ormFile.exists() && ormFile.canRead()) {
						int len = 0;
						int crc32 = 0;
						BufferedReader r = new BufferedReader(
								new InputStreamReader(new FileInputStream(
										ormFile), "utf-8"));
						try {
							String l = r.readLine();
							while (l != null) {
								Matcher m = SIGNATURE.matcher(l);
								if (m.find()) {
									len = Integer.parseInt(m.group(1));
									crc32 = (int) Long
											.parseLong(m.group(2), 16);
									break;
								}
								l = r.readLine();
							}
						} finally {
							r.close();
						}

						if (g.getLength() == len && g.getChecksum() == crc32)
							continue;
					}

					// Перекомпилировать надо и мы начинаем запись.
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
		w.write(String.format(
				"# Source grain parameters: version=%s, len=%d, crc32=%08X.",
				g.getVersion(), g.getLength(), g.getChecksum()));
		w.newLine();
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

		String className = t.getName() + "Cursor";

		w.write(String.format("class %s(Cursor):", className));
		w.newLine();
		for (String s : TABLE_HEADER) {
			w.write(s);
			w.newLine();
		}
		// Конструктор
		w.write("    def __init__(self, context):");
		w.newLine();
		w.write("        Cursor.__init__(self, context)");
		w.newLine();
		for (Column c : columns) {
			w.write(String.format("        self.%s = None", c.getName()));
			w.newLine();
		}
		w.write("        self.context = context");
		w.newLine();
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
		for (Column c : columns) {
			w.write(String.format("        self.%s = rs.%s('%s')", c.getName(),
					c.jdbcGetterName(), c.getName()));
			w.newLine();
		}
		// Очистка буфера
		w.write("    def clearBuffer(self, withKeys):");
		w.newLine();
		w.write("        if withKeys:");
		w.newLine();
		for (Column c : pk) {
			w.write(String.format("            self.%s = None", c.getName()));
			w.newLine();
		}
		for (Column c : columns)
			if (!pk.contains(c)) {
				w.write(String.format("        self.%s = None", c.getName()));
				w.newLine();
			}
		// Текущие значения ключевых полей
		w.write("    def currentKeyValues(self):");
		w.newLine();
		StringBuilder sb = new StringBuilder();
		for (Column c : pk) {
			if (sb.length() > 0)
				sb.append(", ");
			sb.append(String.format("self.%s", c.getName()));
		}
		w.write(String.format("        return array([%s], Object)",
				sb.toString()));
		w.newLine();
		// Текущие значения всех полей
		w.write("    def currentValues(self):");
		w.newLine();
		sb = new StringBuilder();
		for (Column c : columns) {
			if (sb.length() > 0)
				sb.append(", ");
			sb.append(String.format("self.%s", c.getName()));
		}
		w.write(String.format("        return array([%s], Object)",
				sb.toString()));
		w.newLine();
		// Клонирование
		w.write("    def copyFieldsFrom(self, c):");
		w.newLine();
		sb = new StringBuilder();
		for (Column c : columns) {
			w.write(String.format("        self.%s = c.%s", c.getName(),
					c.getName()));
			w.newLine();
		}
		w.write("    def getBufferCopy(self):");
		w.newLine();
		w.write(String.format("        result = %s(self.callContext())", className));
		w.newLine();
		w.write("        result.copyFieldsFrom(self)");
		w.newLine();
		w.write("        return result");
		w.newLine();

		// Триггеры
		w.write("    def preDelete(self):");
		w.newLine();
		w.write(String.format("        for f in %s.onPreDelete:", className));
		w.newLine();
		w.write(String.format(F_SELF));
		w.newLine();
		w.write("    def postDelete(self):");
		w.newLine();
		w.write(String.format("        for f in %s.onPostDelete:", className));
		w.newLine();
		w.write(String.format(F_SELF));
		w.newLine();
		w.write("    def preInsert(self):");
		w.newLine();
		w.write(String.format("        for f in %s.onPreInsert:", className));
		w.newLine();
		w.write(String.format(F_SELF));
		w.newLine();
		w.write("    def postInsert(self):");
		w.newLine();
		w.write(String.format("        for f in %s.onPostInsert:", className));
		w.newLine();
		w.write(String.format(F_SELF));
		w.newLine();
		w.write("    def preUpdate(self):");
		w.newLine();
		w.write(String.format("        for f in %s.onPreUpdate:", className));
		w.newLine();
		w.write(String.format(F_SELF));
		w.newLine();
		w.write("    def postUpdate(self):");
		w.newLine();
		w.write(String.format("        for f in %s.onPostUpdate:", className));
		w.newLine();
		w.write(String.format(F_SELF));
		w.newLine();

		w.newLine();
	}
}

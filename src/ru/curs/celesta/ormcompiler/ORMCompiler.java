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
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.BooleanColumn;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.FloatingColumn;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.GrainElement;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.BinaryColumn;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.StringColumn;
import ru.curs.celesta.score.Table;
import ru.curs.celesta.score.View;
import ru.curs.celesta.score.ViewColumnType;

/**
 * Комилятор ORM-кода.
 */
public final class ORMCompiler {

	private static final String SELF_S_EQUALS_NONE = "        self.%s = None";
	private static final Pattern SIGNATURE = Pattern
			.compile("len=([0-9]+), crc32=([0-9A-F]+)\\.");
	private static final String[] HEADER = {
			"\"\"\"",
			"THIS MODULE IS BEING CREATED AUTOMATICALLY EVERY TIME CELESTA STARTS.",
			"DO NOT MODIFY IT AS YOUR CHANGES WILL BE LOST.", "\"\"\"",
			"import ru.curs.celesta.dbutils.Cursor as Cursor",
			"import ru.curs.celesta.dbutils.ViewCursor as ViewCursor",
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

		for (View v : g.getViews().values())
			compileView(v, w);
	}

	private static void compileView(View v, BufferedWriter w)
			throws IOException {
		String className = v.getName() + "Cursor";

		Map<String, ViewColumnType> columns = v.getColumns();

		w.write(String.format("class %s(ViewCursor):", className));
		w.newLine();
		// Конструктор
		compileViewInit(w, columns);
		// Имя гранулы
		compileGrainName(v, w);
		// Имя таблицы
		compileTableName(v, w);
		// Разбор строки по переменным
		compileParseResult(w, columns);
		// Очистка буфера
		compileClearBuffer(w, columns);
		// Итерация в Python-стиле
		compileIterate(w);
		w.newLine();

	}

	private static void compileClearBuffer(BufferedWriter w,
			Map<String, ViewColumnType> columns) throws IOException {
		w.write("    def _clearBuffer(self, withKeys):");
		w.newLine();
		for (String c : columns.keySet()) {
			w.write(String.format(SELF_S_EQUALS_NONE, c));
			w.newLine();
		}
	}

	private static void compileParseResult(BufferedWriter w,
			Map<String, ViewColumnType> columns) throws IOException {
		w.write("    def _parseResult(self, rs):");
		w.newLine();
		for (Map.Entry<String, ViewColumnType> e : columns.entrySet()) {
			if (e.getValue() == ViewColumnType.BLOB) {
				w.write(String.format(SELF_S_EQUALS_NONE, e.getKey()));
			} else {
				w.write(String.format("        self.%s = rs.%s('%s')",
						e.getKey(), e.getValue().jdbcGetterName(), e.getKey()));
				w.newLine();
				w.write(String.format("        if rs.wasNull():"));
				w.newLine();
				w.write(String.format("    " + SELF_S_EQUALS_NONE, e.getKey()));
			}
			w.newLine();
		}

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
		compileTableInit(w, columns);
		// Имя гранулы
		compileGrainName(t, w);
		// Имя таблицы
		compileTableName(t, w);
		// Разбор строки по переменным
		compileParseResult(w, columns);
		// Очистка буфера
		compileClearBuffer(w, columns, pk);
		// Текущие значения ключевых полей
		compileCurrentKeyValues(w, pk);
		// Текущие значения всех полей
		compileCurrentValues(w, columns);
		// Вычисление BLOB-полей
		compileCalcBLOBs(w, columns);
		// Автоинкремент
		compileSetAutoIncrement(w, columns);
		// Триггеры
		compileTriggers(w, className);
		// Клонирование
		compileCopying(w, columns, className);
		// Итерация в Python-стиле
		compileIterate(w);
		w.newLine();
	}

	private static void compileCalcBLOBs(BufferedWriter w,
			Collection<Column> columns) throws IOException {
		for (Column c : columns)
			if (c instanceof BinaryColumn) {
				w.write(String.format("    def calc%s(self):", c.getName()));
				w.newLine();
				w.write(String.format("        self.%s = self.calcBlob('%s')",
						c.getName(), c.getName()));
				w.newLine();
				w.write(String.format(
						"        self.getXRec().%s = self.%s.clone()",
						c.getName(), c.getName()));
				w.newLine();
			}

	}

	private static void compileTriggers(BufferedWriter w, String className)
			throws IOException {
		w.write("    def _preDelete(self):");
		w.newLine();
		w.write(String.format("        for f in %s.onPreDelete:", className));
		w.newLine();
		w.write(String.format(F_SELF));
		w.newLine();
		w.write("    def _postDelete(self):");
		w.newLine();
		w.write(String.format("        for f in %s.onPostDelete:", className));
		w.newLine();
		w.write(String.format(F_SELF));
		w.newLine();
		w.write("    def _preInsert(self):");
		w.newLine();
		w.write(String.format("        for f in %s.onPreInsert:", className));
		w.newLine();
		w.write(String.format(F_SELF));
		w.newLine();
		w.write("    def _postInsert(self):");
		w.newLine();
		w.write(String.format("        for f in %s.onPostInsert:", className));
		w.newLine();
		w.write(String.format(F_SELF));
		w.newLine();
		w.write("    def _preUpdate(self):");
		w.newLine();
		w.write(String.format("        for f in %s.onPreUpdate:", className));
		w.newLine();
		w.write(String.format(F_SELF));
		w.newLine();
		w.write("    def _postUpdate(self):");
		w.newLine();
		w.write(String.format("        for f in %s.onPostUpdate:", className));
		w.newLine();
		w.write(String.format(F_SELF));
		w.newLine();
	}

	private static void compileCopying(BufferedWriter w,
			Collection<Column> columns, String className) throws IOException {
		w.write("    def _getBufferCopy(self):");
		w.newLine();
		w.write(String.format("        result = %s(self.callContext())",
				className));
		w.newLine();
		w.write("        result.copyFieldsFrom(self)");
		w.newLine();
		w.write("        return result");
		w.newLine();

		w.write("    def copyFieldsFrom(self, c):");
		w.newLine();
		for (Column c : columns) {
			w.write(String.format("        self.%s = c.%s", c.getName(),
					c.getName()));
			w.newLine();
		}
	}

	private static void compileSetAutoIncrement(BufferedWriter w,
			Collection<Column> columns) throws IOException {
		w.write("    def _setAutoIncrement(self, val):");
		w.newLine();
		boolean hasCode = false;
		for (Column c : columns)
			if (c instanceof IntegerColumn && ((IntegerColumn) c).isIdentity()) {
				w.write(String.format("        self.%s = val", c.getName()));

				hasCode = true;
				break;
			}
		if (!hasCode)
			w.write("        pass");
		w.newLine();
	}

	private static void compileCurrentValues(BufferedWriter w,
			Collection<Column> columns) throws IOException {
		w.write("    def _currentValues(self):");
		w.newLine();
		StringBuilder sb = new StringBuilder();
		for (Column c : columns)
			addValue(sb, c);

		w.write(String.format("        return array([%s], Object)",
				sb.toString()));
		w.newLine();
	}

	private static void addValue(StringBuilder sb, Column c) {
		if (sb.length() > 0)
			sb.append(", ");
		if (c instanceof BooleanColumn)
			sb.append(String.format(
					"None if self.%s == None else bool(self.%s)", c.getName(),
					c.getName()));
		else if (c instanceof IntegerColumn)
			sb.append(String.format(
					"None if self.%s == None else int(self.%s)", c.getName(),
					c.getName()));
		else if (c instanceof FloatingColumn)
			sb.append(String.format(
					"None if self.%s == None else float(self.%s)", c.getName(),
					c.getName()));
		else if (c instanceof StringColumn)
			sb.append(String.format(
					"None if self.%s == None else unicode(self.%s)",
					c.getName(), c.getName()));
		else {
			sb.append(String.format("self.%s", c.getName()));
		}
	}

	private static void compileCurrentKeyValues(BufferedWriter w, Set<Column> pk)
			throws IOException {
		w.write("    def _currentKeyValues(self):");
		w.newLine();
		StringBuilder sb = new StringBuilder();
		for (Column c : pk)
			addValue(sb, c);

		w.write(String.format("        return array([%s], Object)",
				sb.toString()));
		w.newLine();
	}

	private static void compileClearBuffer(BufferedWriter w,
			Collection<Column> columns, Set<Column> pk) throws IOException {
		w.write("    def _clearBuffer(self, withKeys):");
		w.newLine();
		w.write("        if withKeys:");
		w.newLine();
		for (Column c : pk) {
			w.write(String.format("            self.%s = None", c.getName()));
			w.newLine();
		}
		for (Column c : columns)
			if (!pk.contains(c)) {
				w.write(String.format(SELF_S_EQUALS_NONE, c.getName()));
				w.newLine();
			}
	}

	private static void compileParseResult(BufferedWriter w,
			Collection<Column> columns) throws IOException {
		w.write("    def _parseResult(self, rs):");
		w.newLine();
		for (Column c : columns) {
			if (c instanceof BinaryColumn) {
				w.write(String.format(SELF_S_EQUALS_NONE, c.getName()));
			} else {
				w.write(String.format("        self.%s = rs.%s('%s')",
						c.getName(), c.jdbcGetterName(), c.getName()));
				w.newLine();
				w.write(String.format("        if rs.wasNull():"));
				w.newLine();
				w.write(String.format("    " + SELF_S_EQUALS_NONE, c.getName()));
			}
			w.newLine();
		}
	}

	private static void compileTableName(GrainElement t, BufferedWriter w)
			throws IOException {
		w.write("    def _tableName(self):");
		w.newLine();
		w.write(String.format("        return '%s'", t.getName()));
		w.newLine();
	}

	private static void compileGrainName(GrainElement t, BufferedWriter w)
			throws IOException {
		w.write("    def _grainName(self):");
		w.newLine();
		w.write(String.format("        return '%s'", t.getGrain().getName()));
		w.newLine();
	}

	private static void compileIterate(BufferedWriter w) throws IOException {
		w.write("    def iterate(self):");
		w.newLine();
		w.write("        if self.tryFirst():");
		w.newLine();
		w.write("            while True:");
		w.newLine();
		w.write("                yield self");
		w.newLine();
		w.write("                if not self.next():");
		w.newLine();
		w.write("                    break");
		w.newLine();
	}

	private static void compileTableInit(BufferedWriter w,
			Collection<Column> columns) throws IOException {
		w.write("    def __init__(self, context):");
		w.newLine();
		w.write("        Cursor.__init__(self, context)");
		w.newLine();
		for (Column c : columns) {
			w.write(String.format(SELF_S_EQUALS_NONE, c.getName()));
			w.newLine();
		}
		w.write("        self.context = context");
		w.newLine();
	}

	private static void compileViewInit(BufferedWriter w,
			Map<String, ViewColumnType> columns) throws IOException {
		w.write("    def __init__(self, context):");
		w.newLine();
		w.write("        ViewCursor.__init__(self, context)");
		w.newLine();
		for (String c : columns.keySet()) {
			w.write(String.format(SELF_S_EQUALS_NONE, c));
			w.newLine();
		}
		w.write("        self.context = context");
		w.newLine();
	}
}

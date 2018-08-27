package ru.curs.celesta.ormcompiler;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.*;

/**
 * Комилятор ORM-кода.
 */
public final class ORMCompiler {

    /**
     * Версия компилятора. Данную константу следует инкрементировать, когда
     * необходимо инициировать автоматическое пересоздание orm-скриптов.
     */
    private static final int COMPILERVER = 23;

    private static final String DEF_CLEAR_BUFFER_SELF_WITH_KEYS = "    def _clearBuffer(self, withKeys):";
    private static final String DEF_INIT_SELF_CONTEXT = "    def __init__(self, context):";
    private static final String DEF_INIT_SELF_CONTEXT_FIELDS = "    def __init__(self, context, fields = []):";
    private static final String DEF_INIT_SELF_CONTEXT_PARAMS_FIELDS_TEMPLATE = "    def __init__(self, context, %s, fields = []):%n";
    private static final String SELF_CONTEXT_CONTEXT = "        self.context = context";
    private static final String RETURN_ARRAY_S_OBJECT = "        return array([%s], Object)%n";
    private static final String SELF_S_EQUALS_NONE = "        self.%s = None%n";

    private static final Pattern SIGNATURE = Pattern
            .compile("len=([0-9]+), crc32=([0-9A-F]+)(; compiler=([0-9]+))?\\.");
    private static final String[] HEADER = {"\"\"\"",
            "THIS MODULE IS BEING CREATED AND UPDATED AUTOMATICALLY.",
            "DO NOT MODIFY IT AS YOUR CHANGES WILL BE LOST.", "\"\"\"",
            "import ru.curs.celesta.dbutils.Cursor as Cursor",
            "import ru.curs.celesta.dbutils.ViewCursor as ViewCursor",
            "import ru.curs.celesta.dbutils.ReadOnlyTableCursor as ReadOnlyTableCursor",
            "import ru.curs.celesta.dbutils.MaterializedViewCursor as MaterializedViewCursor",
            "import ru.curs.celesta.dbutils.ParameterizedViewCursor as ParameterizedViewCursor",
            "import ru.curs.celesta.dbutils.Sequence as Sequence",
            "from ru.curs.celesta.event import TriggerType",
            "import ru.curs.celesta.py.PyTriggerRegistrationAdaptor as TriggerRegister",
            "from java.lang import Object",
            "from jarray import array", "from java.util import Calendar, GregorianCalendar, TimeZone, HashSet, HashMap",
            "from java.sql import Timestamp",
            "from java.time import ZonedDateTime, ZoneOffset",
            "from java.math import BigDecimal",
            "import datetime", "", "def _to_timestamp(d):",
            "    if isinstance(d, datetime.datetime):", "        calendar = GregorianCalendar()",
            "        calendar.set(d.year, d.month - 1, d.day, d.hour, d.minute, d.second)",
            "        ts = Timestamp(calendar.getTimeInMillis())", "        ts.setNanos(d.microsecond * 1000)",
            "        return ts", "    else:", "        return d", ""

    };

    private ORMCompiler() {

    }

    /**
     * Выполняет компиляцию кода на основе разобранной объектной модели.
     *
     * @param score модель
     */
    public static void compile(Score score) {
        for (Grain g : score.getGrains().values())
            // Пропускаем системную гранулу.
            if (!"celesta".equals(g.getName())) {

                for (GrainPart gp: g.getGrainParts()) {
                    File ormFile = new File(String.format(
                            "%s%s_%s_orm.py", gp.getSourceFile().getParentFile().getPath(), File.separator, g.getName()
                    ));
                    try {
                        // Блок проверки: а может, перекомпилировать нет нужды?
                        if (ormFile.exists() && ormFile.canRead()) {
                            int len = 0;
                            int crc32 = 0;
                            int compiler = 0;
                            try (BufferedReader r = new BufferedReader(
                                    new InputStreamReader(new FileInputStream(ormFile), StandardCharsets.UTF_8))) {
                                String l = r.readLine();
                                while (l != null) {
                                    Matcher m = SIGNATURE.matcher(l);
                                    if (m.find()) {
                                        len = Integer.parseInt(m.group(1));
                                        crc32 = (int) Long.parseLong(m.group(2), 16);
                                        compiler = m.group(4) == null ? 0 : Integer.parseInt(m.group(4));
                                        break;
                                    }
                                    l = r.readLine();
                                }
                            }

                            if (g.getLength() == len && g.getChecksum() == crc32 && compiler >= COMPILERVER)
                                continue;
                        }

                        // Перекомпилировать надо и мы начинаем запись.
                        try (
                                PrintWriter w = new PrintWriter(
                                        new OutputStreamWriter(
                                                new FileOutputStream(ormFile), StandardCharsets.UTF_8))) {
                            compileGrain(g, w);
                        }
                        ormFile.setReadable(true, false);
                    } catch (IOException e) {
                        throw new CelestaException("Error while compiling orm classes for '%s' grain: %s", g.getName(),
                                e.getMessage());
                    }
                }
            }
    }

    private static void compileGrain(Grain g, PrintWriter w) {
        w.println("# coding=UTF-8");

        w.printf("# Source grain parameters: version=%s, len=%d, crc32=%08X; compiler=%d.%n", g.getVersion(),
                g.getLength(), g.getChecksum(), COMPILERVER);

        for (String s : HEADER) {
            w.println(s);
        }

        for (Table t : g.getElements(Table.class).values())
            if (t.isReadOnly()) {
                compileROTable(t, w);
            } else {
                compileTable(t, w);
            }

        for (View v : g.getElements(View.class).values())
            compileView(v, w);

        for (MaterializedView v : g.getElements(MaterializedView.class).values())
            compileROTable(v, w);

        for (ParameterizedView pv : g.getElements(ParameterizedView.class).values())
            compileParameterizedView(pv, w);

        for (SequenceElement s : g.getElements(SequenceElement.class).values())
            compileSequence(s, w);
    }

    private static void compileView(View v, PrintWriter w) {
        String className = v.getName() + "Cursor";

        Map<String, ViewColumnMeta> columns = v.getColumns();

        w.printf("class %s(ViewCursor):%n", className);
        // Конструктор
        compileViewInit(w, columns);
        // Имя гранулы
        compileGrainName(v, w);
        // Имя таблицы
        compileObjectName(v, w);
        // Разбор строки по переменным
        compileParseResult(w, columns);
        // Динамическая установка значения поля
        compileSetFieldValue(w);
        // Очистка буфера
        compileClearBuffer(w, columns);
        // Текущие значения всех полей
        compileCurrentValues(w, columns);
        // Клонирование
        compileCopying(w, v.getColumns().keySet(), className);
        // Итерация в Python-стиле
        compileIterate(w);
        w.println();
    }

    private static void compileParameterizedView(ParameterizedView pv, PrintWriter w) {
        String className = pv.getName() + "Cursor";

        Map<String, ViewColumnMeta> columns = pv.getColumns();

        w.printf("class %s(ParameterizedViewCursor):%n", className);
        // Конструктор
        compileParameterizedViewInit(w, columns, pv);
        // Имя гранулы
        compileGrainName(pv, w);
        // Имя таблицы
        compileObjectName(pv, w);
        // Разбор строки по переменным
        compileParseResult(w, columns);
        // Динамическая установка значения поля
        compileSetFieldValue(w);
        // Очистка буфера
        compileClearBuffer(w, columns);
        // Текущие значения всех полей
        compileCurrentValues(w, columns);
        // Клонирование
        compileCopying(w, pv.getColumns().keySet(), className);
        // Итерация в Python-стиле
        compileIterate(w);
        w.println();
    }

    private static void compileSequence(SequenceElement s , PrintWriter w) {
        String className = s.getName() + "Sequence";

        w.printf("class %s(Sequence):%n", className);
        //constructor
        compileSequenceInit(w);
        // _grainName()
        compileGrainName(s, w);
        // _objectName
        compileObjectName(s, w);

        w.println();
    }

    private static void compileClearBuffer(PrintWriter w, Map<String, ViewColumnMeta> columns) {
        w.println(DEF_CLEAR_BUFFER_SELF_WITH_KEYS);
        for (String c : columns.keySet()) {
            w.printf(SELF_S_EQUALS_NONE, c);
        }
    }

    private static void compileParseResult(PrintWriter w, Map<String, ViewColumnMeta> columns) {
        w.println("    def _parseResult(self, rs):");
        for (Map.Entry<String, ViewColumnMeta> e : columns.entrySet()) {
            if (e.getValue().getColumnType() == ViewColumnType.BLOB) {
                w.printf(SELF_S_EQUALS_NONE, e.getKey());
            } else {
                w.printf("        if self.inRec('%s'):%n", e.getKey());
                w.printf("            self.%s = rs.%s('%s')%n",
                        e.getKey(), e.getValue().jdbcGetterName(), e.getKey());
                w.printf("            if rs.wasNull():%n");
                w.printf("        " + SELF_S_EQUALS_NONE, e.getKey());
            }
        }
    }

    static void compileROTable(TableElement t, PrintWriter w) {
        String className = t.getName() + "Cursor";

        Collection<Column> columns = t.getColumns().values();

        if (t instanceof Table) {
            w.printf("class %s(ReadOnlyTableCursor):%n", className);
        } else {
            w.printf("class %s(MaterializedViewCursor):%n", className);
        }

        // Option-поля
        compileOptionFields(w, columns);
        // Конструктор
        if (t instanceof Table) {
            compileROTableInit(w, columns);
        } else {
            compileMaterializedViewInit(w, columns);
        }
        // Имя гранулы
        compileGrainName((GrainElement) t, w);
        // Имя таблицы
        compileObjectName((GrainElement) t, w);
        // Разбор строки по переменным
        compileParseResult(w, columns, true);
        // Динамическая установка значения поля
        compileSetFieldValue(w);
        // Очистка буфера
        compileClearBuffer(w, columns);

        if (t instanceof MaterializedView) {
            MaterializedView mv = (MaterializedView)t;
            Set<Column> pk = new LinkedHashSet<>(mv.getPrimaryKey().values());
            compileCurrentKeyValues(w, pk);
        }

        // Текущие значения всех полей
        compileCurrentValues(w, columns);
        // Клонирование
        compileCopying(w, t.getColumns().keySet(), className);
        // Итерация в Python-стиле
        compileIterate(w);
        w.println();
    }

    static void compileTable(Table t, PrintWriter w) {

        Collection<Column> columns = t.getColumns().values();
        Set<Column> pk = new LinkedHashSet<>(t.getPrimaryKey().values());

        String className = t.getName() + "Cursor";

        w.printf("class %s(Cursor):%n", className);
        //Методы регистрации триггеров
        compileTriggers(w, className);
        // Option-поля
        compileOptionFields(w, columns);
        // Конструктор
        compileTableInit(w, columns);
        // Имя гранулы
        compileGrainName(t, w);
        // Имя таблицы
        compileObjectName(t, w);
        // Разбор строки по переменным
        compileParseResult(w, columns, false);
        if (t.isVersioned()) {
            w.println("        self.recversion = rs.getInt('recversion')");
        }
        // Динамическая установка значения поля
        compileSetFieldValue(w);

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
        // Клонирование
        compileCopying(w, t.getColumns().keySet(), className);
        if (t.isVersioned()) {
            w.println("        self.recversion = c.recversion");
        }

        // Итерация в Python-стиле
        compileIterate(w);

        w.println();
    }

    private static void compileOptionFields(PrintWriter w, Collection<Column> columns) {
        for (Column c : columns) {
            if (c instanceof IntegerColumn || c instanceof StringColumn) {
                try {
                    List<String> options = c.getOptions();

                    if (options.isEmpty()) {
                        continue;
                    }

                    w.printf("    class %s:%n", c.getName());
                    int j = 0;
                    for (String option : options) {
                        if (c instanceof IntegerColumn) {
                            w.printf("        %s = %d%n", option, j++);
                        } else {
                            w.printf("        %s = '%s'%n", option, option);
                        }
                    }
                } catch (CelestaException e) {
                    // do nothing -- no options to produce
                }

            }
        }

    }

    private static void compileSetFieldValue(PrintWriter w) {
        w.println("    def _setFieldValue(self, name, value):");
        w.println("        setattr(self, name, value)");
    }

    private static void compileCalcBLOBs(PrintWriter w, Collection<Column> columns) {
        for (Column c : columns)
            if (c instanceof BinaryColumn) {
                w.printf("    def calc%s(self):%n", c.getName());
                w.printf("        self.%s = self.calcBlob('%s')%n", c.getName(), c.getName());
                w.printf("        self.getXRec().%s = self.%s.clone()%n", c.getName(), c.getName());
            }
    }

    private static void compileTriggers(PrintWriter w, String className) {
        w.println("    @staticmethod");
        w.println("    def onPreDelete(celesta, f):");
        w.printf("        TriggerRegister.registerTrigger(celesta, TriggerType.PRE_DELETE, f, %s)%n", className);
        w.println("    @staticmethod");
        w.println("    def onPostDelete(celesta, f):");
        w.printf("        TriggerRegister.registerTrigger(celesta, TriggerType.POST_DELETE, f, %s)%n", className);
        w.println("    @staticmethod");
        w.println("    def onPreInsert(celesta, f):");
        w.printf("        TriggerRegister.registerTrigger(celesta, TriggerType.PRE_INSERT, f, %s)%n", className);
        w.println("    @staticmethod");
        w.println("    def onPostInsert(celesta, f):");
        w.printf("        TriggerRegister.registerTrigger(celesta, TriggerType.POST_INSERT, f, %s)%n", className);
        w.println("    @staticmethod");
        w.println("    def onPreUpdate(celesta, f):");
        w.printf("        TriggerRegister.registerTrigger(celesta, TriggerType.PRE_UPDATE, f, %s)%n", className);
        w.println("    @staticmethod");
        w.println("    def onPostUpdate(celesta, f):");
        w.printf("        TriggerRegister.registerTrigger(celesta, TriggerType.POST_UPDATE, f, %s)%n", className);
    }

    private static void compileCopying(PrintWriter w, Collection<String> columns, String className) {
        w.println("    def _getBufferCopy(self, context, fields=None):");
        w.printf("        result = %s(context, fields)%n", className);
        w.println("        result.copyFieldsFrom(self)");
        w.println("        return result");

        w.println("    def copyFieldsFrom(self, c):");
        for (String c : columns) {
            w.printf("        self.%s = c.%s%n", c, c);
        }
    }

    private static void compileSetAutoIncrement(PrintWriter w, Collection<Column> columns) {
        w.println("    def _setAutoIncrement(self, val):");
        boolean hasCode = false;
        for (Column c : columns)
            if (c instanceof IntegerColumn) {
                IntegerColumn ic = (IntegerColumn)c;
                if (ic.isIdentity() || ic.getSequence() != null) {
                    w.printf("        self.%s = val%n", c.getName());
                    hasCode = true;
                    break;
                }
            }
        if (!hasCode)
            w.println("        pass");
    }

    private static void compileCurrentValues(PrintWriter w, Collection<Column> columns) {
        w.println("    def _currentValues(self):");
        StringBuilder sb = new StringBuilder();
        for (Column c : columns)
            addValue(sb, c);

        w.printf(RETURN_ARRAY_S_OBJECT, sb.toString());
    }

    private static void compileCurrentValues(PrintWriter w, Map<String, ViewColumnMeta> columns) {
        w.println("    def _currentValues(self):");
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, ViewColumnMeta> c : columns.entrySet())
            addValue(sb, c);

        w.printf(RETURN_ARRAY_S_OBJECT, sb.toString());
    }

    private static void addValue(StringBuilder sb, Column c) {
        if (sb.length() > 0)
            sb.append(", ");
        if (c instanceof BooleanColumn)
            sb.append(String.format("None if self.%s == None else bool(self.%s)", c.getName(), c.getName()));
        else if (c instanceof IntegerColumn)
            sb.append(String.format("None if self.%s == None else int(self.%s)", c.getName(), c.getName()));
        else if (c instanceof FloatingColumn)
            sb.append(String.format("None if self.%s == None else float(self.%s)", c.getName(), c.getName()));
        else if (c instanceof StringColumn)
            sb.append(String.format("None if self.%s == None else unicode(self.%s)", c.getName(), c.getName()));
        else if (c instanceof DateTimeColumn)
            sb.append(String.format("_to_timestamp(self.%s)", c.getName()));
        else {
            sb.append(String.format("self.%s", c.getName()));
        }
    }

    private static void addValue(StringBuilder sb, Entry<String, ViewColumnMeta> c) {
        if (sb.length() > 0)
            sb.append(", ");
        if (c.getValue().getColumnType() == ViewColumnType.BIT)
            sb.append(String.format("None if self.%s == None else bool(self.%s)", c.getKey(), c.getKey()));
        else if (c.getValue().getColumnType() == ViewColumnType.INT)
            sb.append(String.format("None if self.%s == None else int(self.%s)", c.getKey(), c.getKey()));
        else if (c.getValue().getColumnType() == ViewColumnType.REAL)
            sb.append(String.format("None if self.%s == None else float(self.%s)", c.getKey(), c.getKey()));
        else if (c.getValue().getColumnType() == ViewColumnType.DECIMAL)
            sb.append(String.format("None if self.%s == None else BigDecimal(self.%s)", c.getKey(), c.getKey()));
        else if (c.getValue().getColumnType() == ViewColumnType.TEXT)
            sb.append(String.format("None if self.%s == None else unicode(self.%s)", c.getKey(), c.getKey()));
        else {
            sb.append(String.format("self.%s", c.getKey()));
        }
    }

    private static void compileCurrentKeyValues(PrintWriter w, Set<Column> pk) {
        w.println("    def _currentKeyValues(self):");
        StringBuilder sb = new StringBuilder();
        for (Column c : pk)
            addValue(sb, c);

        w.printf(RETURN_ARRAY_S_OBJECT, sb.toString());
    }

    private static void compileClearBuffer(PrintWriter w, Collection<Column> columns, Set<Column> pk) {
        w.println(DEF_CLEAR_BUFFER_SELF_WITH_KEYS);
        w.println("        if withKeys:");
        for (Column c : pk) {
            w.printf("            self.%s = None%n", c.getName());
        }
        for (Column c : columns)
            if (!pk.contains(c)) {
                w.printf(SELF_S_EQUALS_NONE, c.getName());
            }
    }

    private static void compileClearBuffer(PrintWriter w, Collection<Column> columns) {
        w.println(DEF_CLEAR_BUFFER_SELF_WITH_KEYS);
        for (Column c : columns) {
            w.printf(SELF_S_EQUALS_NONE, c.getName());
        }
    }

    private static void compileParseResult(PrintWriter w, Collection<Column> columns, boolean isReadOnlyTable) {
        String methodName = isReadOnlyTable ? "_parseResult" : "_parseResultInternal";
        w.printf("    def %s(self, rs):%n", methodName);
        for (Column c : columns) {
            if (c instanceof BinaryColumn) {
                w.printf(SELF_S_EQUALS_NONE, c.getName());
            } else {
                w.printf("        if self.inRec('%s'):%n", c.getName());
                if (c instanceof ZonedDateTimeColumn) {
                    w.printf(
                            "            self.%s = rs.%s('%s', Calendar.getInstance(TimeZone.getTimeZone(\"UTC\")))%n",
                            c.getName(), c.jdbcGetterName(), c.getName()
                    );
                    w.printf(
                            "            if not rs.wasNull():%n"
                    );
                    w.printf(
                            "                self.%s = ZonedDateTime.of(self.%s.toLocalDateTime(), ZoneOffset.systemDefault())%n",
                            c.getName(), c.getName()
                    );
                } else {
                    w.printf("            self.%s = rs.%s('%s')%n", c.getName(), c.jdbcGetterName(), c.getName());
                    w.printf("            if rs.wasNull():%n");
                    w.printf("        " + SELF_S_EQUALS_NONE, c.getName());
                }
            }
        }
    }

    private static void compileObjectName(GrainElement t, PrintWriter w) {
        w.println("    def _objectName(self):");
        w.printf("        return '%s'%n", t.getName());
    }

    private static void compileGrainName(GrainElement t, PrintWriter w) {
        w.println("    def _grainName(self):");
        w.printf("        return '%s'%n", t.getGrain().getName());
    }

    private static void compileIterate(PrintWriter w) {
        w.println("    def iterate(self):");
        w.println("        if self.tryFindSet():");
        w.println("            while True:");
        w.println("                yield self");
        w.println("                if not self.nextInSet():");
        w.println("                    break");
    }

    private static void compileTableInit(PrintWriter w, Collection<Column> columns) {
        w.println(DEF_INIT_SELF_CONTEXT_FIELDS);
        w.println("        if fields:");
        w.println("            Cursor.__init__(self, context, HashSet(fields))");
        w.println("        else:");
        w.println("            Cursor.__init__(self, context)");
        for (Column c : columns) {
            w.printf(SELF_S_EQUALS_NONE, c.getName());
        }
        w.println(SELF_CONTEXT_CONTEXT);
    }

    private static void compileROTableInit(PrintWriter w, Collection<Column> columns) {
        w.println(DEF_INIT_SELF_CONTEXT_FIELDS);
        w.println("        if fields:");
        w.println("            ReadOnlyTableCursor.__init__(self, context, HashSet(fields))");
        w.println("        else:");
        w.println("            ReadOnlyTableCursor.__init__(self, context)");
        for (Column c : columns) {
            w.printf(SELF_S_EQUALS_NONE, c.getName());
        }
        w.println(SELF_CONTEXT_CONTEXT);
    }

    private static void compileMaterializedViewInit(PrintWriter w, Collection<Column> columns) {
        w.println(DEF_INIT_SELF_CONTEXT_FIELDS);
        w.println("        if fields:");
        w.println("            MaterializedViewCursor.__init__(self, context, HashSet(fields))");
        w.println("        else:");
        w.println("            MaterializedViewCursor.__init__(self, context)");
        for (Column c : columns) {
            w.printf(SELF_S_EQUALS_NONE, c.getName());
        }
        w.println(SELF_CONTEXT_CONTEXT);
    }

    private static void compileViewInit(PrintWriter w, Map<String, ViewColumnMeta> columns) {
        w.println(DEF_INIT_SELF_CONTEXT_FIELDS);
        w.println("        if fields:");
        w.println("            ViewCursor.__init__(self, context, HashSet(fields))");
        w.println("        else:");
        w.println("            ViewCursor.__init__(self, context)");
        for (String c : columns.keySet()) {
            w.printf(SELF_S_EQUALS_NONE, c);
        }
        w.println(SELF_CONTEXT_CONTEXT);
    }

    private static void compileParameterizedViewInit(
            PrintWriter w,
            Map<String, ViewColumnMeta> columns,
            ParameterizedView pv
    ) {
        String params = pv.getParameters().keySet().stream().collect(Collectors.joining(", "));
        w.printf(DEF_INIT_SELF_CONTEXT_PARAMS_FIELDS_TEMPLATE, params);
        w.println("        params = HashMap()");
        for (String param : pv.getParameters().keySet()) {
            w.println("        params.put('" + param + "', " + param + ")");
        }

        w.println("        if fields:");
        w.println("            ParameterizedViewCursor.__init__(self, context, HashSet(fields), params)");
        w.println("        else:");
        w.println("            ParameterizedViewCursor.__init__(self, context, params)");
        for (String c : columns.keySet()) {
            w.printf(SELF_S_EQUALS_NONE, c);
        }
        w.println(SELF_CONTEXT_CONTEXT);
    }

    private static void compileSequenceInit(PrintWriter w) {
        w.println(DEF_INIT_SELF_CONTEXT);
        w.println("        Sequence.__init__(self, context)");
        w.println(SELF_CONTEXT_CONTEXT);
    }
}

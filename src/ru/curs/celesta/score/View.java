package ru.curs.celesta.score;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Объект-представление в метаданных.
 */
public class View extends GrainElement {
	private boolean distinct;
	private final Map<String, Expr> columns = new LinkedHashMap<>();
	private Map<String, ViewColumnType> columnTypes = null;
	private final Map<String, TableRef> tables = new LinkedHashMap<>();
	private Expr whereCondition;
	private String queryString;

	View(Grain grain, String name) throws ParseException {
		super(grain, name);
		grain.addView(this);
	}

	public View(Grain grain, String name, String sql) throws ParseException {
		this(grain, name);
		StringReader sr = new StringReader(sql);
		CelestaParser parser = new CelestaParser(sr);
		try {
			try {
				parser.select(this);
			} finally {
				sr.close();
			}
			finalizeParsing();
		} catch (ParseException e) {
			delete();
			throw e;
		}
	}

	/**
	 * Использовано ли слово DISTINCT в запросе представления.
	 */
	boolean isDistinct() {
		return distinct;
	}

	/**
	 * Устанавливает использование слова DISTINCT в запросе представления.
	 * 
	 * @param distinct
	 *            Если запрос имеет вид SELECT DISTINCT.
	 */
	void setDistinct(boolean distinct) {
		this.distinct = distinct;
	}

	/**
	 * Добавляет колонку к представлению.
	 * 
	 * @param alias
	 *            Алиас колонки.
	 * @param expr
	 *            Выражение колонки.
	 * @throws ParseException
	 *             Неуникальное имя алиаса или иная семантическая ошибка
	 */
	void addColumn(String alias, Expr expr) throws ParseException {
		if (expr == null)
			throw new IllegalArgumentException();

		if (alias == null || alias.isEmpty())
			throw new ParseException(String.format("View '%s' contains a column with undefined alias.", getName()));
		if (columns.containsKey(alias))
			throw new ParseException(String.format(
					"View '%s' already contains column with name or alias '%s'. Use unique aliases for view columns.",
					getName(), alias));

		columns.put(alias, expr);
	}

	/**
	 * Добавляет ссылку на таблицу к представлению.
	 * 
	 * @param ref
	 *            Ссылка на таблицу.
	 * @throws ParseException
	 *             Неуникальный алиас или иная ошибка.
	 */
	void addFromTableRef(TableRef ref) throws ParseException {
		if (ref == null)
			throw new IllegalArgumentException();

		String alias = ref.getAlias();
		if (alias == null || alias.isEmpty())
			throw new ParseException(String.format("View '%s' contains a table with undefined alias.", getName()));
		if (tables.containsKey(alias))
			throw new ParseException(String.format(
					"View '%s' already contains table with name or alias '%s'. Use unique aliases for view tables.",
					getName(), alias));

		tables.put(alias, ref);

		Expr onCondition = ref.getOnExpr();
		if (onCondition != null) {
			onCondition.resolveFieldRefs(new ArrayList<>(tables.values()));
			onCondition.validateTypes();
		}
	}

	/**
	 * Возвращает перечень столбцов представления.
	 */
	public final Map<String, ViewColumnType> getColumns() {
		if (columnTypes == null) {
			columnTypes = new LinkedHashMap<>();
			for (Entry<String, Expr> e : columns.entrySet())
				columnTypes.put(e.getKey(), e.getValue().getType());
		}
		return columnTypes;
	}

	/**
	 * Финализирует разбор представления, разрешая ссылки на поля и проверяя
	 * типы выражений.
	 * 
	 * @throws ParseException
	 *             ошибка проверки типов или разрешения ссылок.
	 */
	void finalizeParsing() throws ParseException {
		List<TableRef> t = new ArrayList<>(tables.values());
		for (Expr e : columns.values()) {
			e.resolveFieldRefs(t);
			e.validateTypes();
		}
		if (whereCondition != null) {
			whereCondition.resolveFieldRefs(t);
			whereCondition.validateTypes();
		}
	}

	/**
	 * Возвращает условие where для SQL-запроса.
	 */
	Expr getWhereCondition() {
		return whereCondition;
	}

	/**
	 * Устанавливает условие where для SQL-запроса.
	 * 
	 * @param whereCondition
	 *            условие where.
	 * @throws ParseException
	 *             если тип выражения неверный.
	 */
	void setWhereCondition(Expr whereCondition) throws ParseException {
		if (whereCondition != null) {
			List<TableRef> t = new ArrayList<>(tables.values());
			whereCondition.resolveFieldRefs(t);
			whereCondition.assertType(ViewColumnType.LOGIC);
		}
		this.whereCondition = whereCondition;
	}

	private void selectScript(BufferedWriter bw, SQLGenerator gen) throws IOException {
		bw.write("  select ");
		if (distinct)
			bw.write("distinct ");
		boolean cont = false;
		for (Map.Entry<String, Expr> e : columns.entrySet()) {
			if (cont)
				bw.write(", ");
			bw.write(gen.generateSQL(e.getValue()));
			bw.write(" as ");
			if (gen.quoteNames())
				bw.write("\"");
			bw.write(e.getKey());
			if (gen.quoteNames())
				bw.write("\"");
			cont = true;
		}
		bw.newLine();
		bw.write("  from ");
		cont = false;
		for (TableRef tRef : tables.values()) {
			if (cont) {
				bw.newLine();
				bw.write(String.format("    %s ", tRef.getJoinType().toString()));
				bw.write("join ");
			}
			bw.write(gen.tableName(tRef));
			if (cont) {
				bw.write(" on ");
				bw.write(gen.generateSQL(tRef.getOnExpr()));
			}
			cont = true;
		}
		if (whereCondition != null) {
			bw.newLine();
			bw.write("  where ");
			bw.write(gen.generateSQL(whereCondition));
		}
	}

	/**
	 * Создаёт скрипт CREATE VIEW в различных диалектах SQL, используя паттерн
	 * visitor.
	 * 
	 * @param bw
	 *            поток, в который происходит сохранение.
	 * @param gen
	 *            генератор-visitor
	 * @throws IOException
	 *             ошибка записи в поток
	 */
	public void createViewScript(BufferedWriter bw, SQLGenerator gen) throws IOException {
		bw.write(gen.preamble(this));
		bw.newLine();
		selectScript(bw, gen);
	}

	/**
	 * Генератор CelestaSQL.
	 */
	private class CelestaSQLGen extends SQLGenerator {
		@Override
		protected String preamble(View view) {
			return String.format("create view %s as", viewName(view));
		}

		@Override
		protected String viewName(View v) {
			return getName();
		}

		@Override
		protected String tableName(TableRef tRef) {
			Table t = tRef.getTable();
			if (t.getGrain() == getGrain()) {
				return String.format("%s as %s", t.getName(), tRef.getAlias());
			} else {
				return String.format("%s.%s as %s", t.getGrain().getName(), t.getName(), tRef.getAlias());
			}
		}

		@Override
		protected boolean quoteNames() {
			return false;
		}

	}

	void save(BufferedWriter bw) throws IOException {
		SQLGenerator gen = new CelestaSQLGen();
		Grain.writeCelestaDoc(this, bw);
		createViewScript(bw, gen);
		bw.write(";");
		bw.newLine();
		bw.newLine();
	}

	/**
	 * Удаляет таблицу.
	 * 
	 * @throws ParseException
	 *             при попытке изменить системную гранулу
	 */
	public void delete() throws ParseException {
		getGrain().removeView(this);
	}

	/**
	 * Возвращает SQL-запрос на языке Celesta, на основании которого построено
	 * представление.
	 */
	public String getCelestaQueryString() {
		if (queryString != null)
			return queryString;
		StringWriter sw = new StringWriter();
		BufferedWriter bw = new BufferedWriter(sw);
		SQLGenerator gen = new CelestaSQLGen();
		try {
			selectScript(bw, gen);
			bw.flush();
		} catch (IOException e) {
			// This should never happen for in-memory streams
			throw new RuntimeException(e);
		}

		queryString = sw.toString();
		return queryString;
	}
}
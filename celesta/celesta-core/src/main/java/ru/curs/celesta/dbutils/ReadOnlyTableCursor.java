package ru.curs.celesta.dbutils;

import java.util.List;
import java.util.Set;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.Celesta;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Table;

/**
 * Курсор для таблиц, определённых только для чтения.
 */
public abstract class ReadOnlyTableCursor extends BasicCursor {
	private Table meta = null;

	public ReadOnlyTableCursor(CallContext context) throws CelestaException {
		super(context);
	}

	public ReadOnlyTableCursor(CallContext context, Set<String> fields) throws CelestaException {
		super(context, fields);
	}

	@Override
	public final Table meta() throws CelestaException {
		if (meta == null)
			try {
				meta = callContext().getScore()
						.getGrain(_grainName()).getElement(_tableName(), Table.class);
			} catch (ParseException e) {
				throw new CelestaException(e.getMessage());
			}
		return meta;
	}

	@Override
	final void appendPK(List<String> l, List<Boolean> ol, Set<String> colNames) throws CelestaException {

		if (meta().getPrimaryKey().isEmpty() && colNames.isEmpty()) {
			// Если никакой сортировки нет вовсе, сортируем по первому полю.
			l.add(String.format("\"%s\"", meta().getColumns().keySet().iterator().next()));
			ol.add(Boolean.FALSE);
		} else {
			// Всегда добавляем в конец OrderBy поля первичного ключа, идующие в
			// естественном порядке
			for (String colName : meta().getPrimaryKey().keySet())
				if (!colNames.contains(colName)) {
					l.add(String.format("\"%s\"", colName));
					ol.add(Boolean.FALSE);
				}
		}
	}
}

package ru.curs.celesta.dbutils;

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

	@Override
	public final Table meta() throws CelestaException {
		if (meta == null)
			try {
				meta = Celesta.getInstance().getScore().getGrain(_grainName())
						.getTable(_tableName());
			} catch (ParseException e) {
				throw new CelestaException(e.getMessage());
			}
		return meta;
	}

	@Override
	void appendPK(StringBuilder orderByClause, boolean needComma,
			Set<String> colNames) throws CelestaException {
		boolean nc = needComma;
		if (meta().getPrimaryKey().isEmpty() && colNames.isEmpty()) {
			// Если никакой сортировки нет вовсе, сортируем по первому полю.
			if (needComma)
				orderByClause.append(", ");
			orderByClause.append(String.format("\"%s\"", meta().getColumns()
					.keySet().iterator().next()));
		} else {
			// Всегда добавляем в конец OrderBy поля первичного ключа, идующие в
			// естественном порядке
			for (String colName : meta().getPrimaryKey().keySet())
				if (!colNames.contains(colName)) {
					if (nc)
						orderByClause.append(", ");
					orderByClause.append(String.format("\"%s\"", colName));
					nc = true;
				}
		}
	}
}

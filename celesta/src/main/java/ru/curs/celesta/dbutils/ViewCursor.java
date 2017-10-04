package ru.curs.celesta.dbutils;

import java.util.List;
import java.util.Set;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.Celesta;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.View;

/**
 * Базовый класс курсора для посмотра данных в представлениях.
 */
public abstract class ViewCursor extends BasicCursor {

	private View meta = null;

	public ViewCursor(CallContext context) throws CelestaException {
		super(context);
	}

	public ViewCursor(CallContext context, Set<String> fields) throws CelestaException {
		super(context, fields);
	}

	/**
	 * Описание представления (метаинформация).
	 * 
	 * @throws CelestaException
	 *             в случае ошибки извлечения метаинформации (в норме не должна
	 *             происходить).
	 */
	@Override
	public View meta() throws CelestaException {
		if (meta == null)
			try {
				meta = callContext().getScore()
						.getGrain(_grainName()).getElement(_tableName(), View.class);
			} catch (ParseException e) {
				throw new CelestaException(e.getMessage());
			}
		return meta;
	}

	@Override
	final void appendPK(List<String> l, List<Boolean> ol, Set<String> colNames) throws CelestaException {
		// для представлений мы сортируем всегда по первому столбцу, если
		// сортировки нет вообще
		if (colNames.isEmpty()) {
			l.add(String.format("\"%s\"", meta().getColumns().keySet().iterator().next()));
			ol.add(Boolean.FALSE);
		}
	}

}

package ru.curs.celesta.dbutils;

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
				meta = Celesta.getInstance().getScore().getGrain(_grainName())
						.getView(_tableName());
			} catch (ParseException e) {
				throw new CelestaException(e.getMessage());
			}
		return meta;
	}

}

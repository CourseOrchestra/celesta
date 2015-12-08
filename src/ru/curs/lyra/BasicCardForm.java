package ru.curs.lyra;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Date;

import org.python.core.PyObject;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.Cursor;
import ru.curs.celesta.score.ParseException;

/**
 * Base Java class for Lyra card form.
 */
public abstract class BasicCardForm extends BasicLyraForm {

	private static final String UTF_8 = "utf-8";
	private LyraFormData lfd;

	public BasicCardForm(CallContext context) throws CelestaException, ParseException {
		super(context);
	}

	/**
	 * Отыскивает первую запись в наборе записей.
	 * 
	 * @throws CelestaException
	 *             Ошибка извлечения данных из базы.
	 * @throws ParseException
	 *             Ошибка сериализации.
	 */
	public String findRec() throws CelestaException, ParseException {
		ByteArrayOutputStream result = new ByteArrayOutputStream();
		serialize(rec(), result);
		try {
			return result.toString(UTF_8);
		} catch (UnsupportedEncodingException e) {
			return "";
		}
	}

	/**
	 * Отменяет текущие изменения в курсоре и возвращает актуальную информацию
	 * из базы данных.
	 * 
	 * @param data
	 *            сериализованный курсор
	 * 
	 * @throws CelestaException
	 *             Ошибка извлечения данных из базы.
	 * @throws ParseException
	 *             Ошибка сериализации.
	 */
	public String revert(String data) throws CelestaException, ParseException {

		Cursor c = getCursor();

		ByteArrayInputStream dataIS;
		try {
			dataIS = new ByteArrayInputStream(data.getBytes(UTF_8));
			deserialize(c, dataIS);
			c.navigate("=<>");
			ByteArrayOutputStream result = new ByteArrayOutputStream();
			serialize(c, result);
			return result.toString(UTF_8);
		} catch (UnsupportedEncodingException e) {
			return "";
		}
	}

	/**
	 * Перемещает курсор.
	 * 
	 * @param cmd
	 *            Команда перемещения (комбинация знаков <, >, =, +, -, см.
	 *            документацию по методу курсора navigate)
	 * 
	 * @param data
	 *            сериализованный курсор.
	 * 
	 * @throws CelestaException
	 *             Ошибка извлечения данных из базы.
	 * @throws ParseException
	 *             Ошибка сериализации.
	 */
	public String move(String cmd, String data) throws CelestaException, ParseException {
		try {
			if (rec() instanceof Cursor) {
				Cursor c = getCursor();
				ByteArrayInputStream dataIS = new ByteArrayInputStream(data.getBytes(UTF_8));
				deserialize(c, dataIS);

				Cursor c2 = getCursor();
				c2.copyFieldsFrom(c);
				if (c2.tryGetCurrent()) {
					c2.copyFieldsFrom(c);
					c2.update();
				} else {
					c.insert();
				}
			}
			rec().navigate(cmd);
			ByteArrayOutputStream result = new ByteArrayOutputStream();
			serialize(rec(), result);
			return result.toString(UTF_8);
		} catch (UnsupportedEncodingException e) {
			return "";
		}
	}

	/**
	 * Инициирует новую запись для вставки в базу данных.
	 * 
	 * @throws CelestaException
	 *             Ошибка извлечения данных из базы.
	 * @throws ParseException
	 *             Ошибка сериализации.
	 */
	public String newRec() throws CelestaException, ParseException {
		Cursor c = getCursor();
		c.clear();
		c.setRecversion(0);
		ByteArrayOutputStream result = new ByteArrayOutputStream();
		serialize(c, result);
		try {
			return result.toString(UTF_8);
		} catch (UnsupportedEncodingException e) {
			return "";
		}
	}

	/**
	 * Удаляет текущую запись.
	 * 
	 * @param data
	 *            сериализованный курсор.
	 * 
	 * @throws CelestaException
	 *             Ошибка извлечения данных из базы.
	 * @throws ParseException
	 *             Ошибка сериализации.
	 */
	public String deleteRec(String data) throws CelestaException, ParseException {
		Cursor c = getCursor();

		ByteArrayInputStream dataIS;
		try {
			dataIS = new ByteArrayInputStream(data.getBytes(UTF_8));

			deserialize(c, dataIS);

			c.delete();
			if (!c.navigate(">+")) {
				c.clear();
				c.setRecversion(0);
			}
			ByteArrayOutputStream result = new ByteArrayOutputStream();
			serialize(c, result);
			return result.toString(UTF_8);
		} catch (UnsupportedEncodingException e) {
			return "";
		}
	}

	private void serialize(BasicCursor c, OutputStream result) throws CelestaException, ParseException {
		_beforeSending(c);
		lfd = new LyraFormData(c, _getId());
		// добавление полей формы
		_serializeFields();
		lfd.serialize(result);
	}

	private void deserialize(Cursor c, InputStream dataIS) throws CelestaException {
		lfd = new LyraFormData(dataIS);
		lfd.populateFields(c);
		for (LyraFieldValue v : lfd.getFields())
			if (v.isLocal()) {
				_restoreValue(v.getName(), v.getValue());
			}

		_afterReceiving(c);
	}

	// CHECKSTYLE:OFF
	/*
	 * This methods are named in Python style, not Java style. This is why
	 * methods meant to be protected are called starting from underscore.
	 */
	protected void _saveFieldValue(String celestatype, String name, PyObject value, String caption)
			throws ParseException {
		LyraFieldType lft = LyraFieldType.valueOf(celestatype);
		switch (lft) {
		case BIT:
			Boolean b = (Boolean) value.__tojava__(Boolean.class);
			lfd.addValue(name, b, true);
			break;
		case DATETIME:
			Date d = (Date) value.__tojava__(Date.class);
			lfd.addValue(name, d, true);
			break;
		case REAL:
			Double dbl = (Double) value.__tojava__(Double.class);
			lfd.addValue(name, dbl, true);
			break;
		case INT:
			Integer i = (Integer) value.__tojava__(Integer.class);
			lfd.addValue(name, i, true);
			break;
		case VARCHAR:
			String s = (String) value.__tojava__(String.class);
			lfd.addValue(name, s, true);
			break;
		default:
			break;
		}

	}

	public abstract void _beforeSending(BasicCursor c);

	public abstract void _afterReceiving(BasicCursor c);

	public abstract void _serializeFields();

	public abstract void _restoreValue(String name, Object value);
	// CHECKSTYLE:ON
}

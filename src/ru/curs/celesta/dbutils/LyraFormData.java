package ru.curs.celesta.dbutils;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map.Entry;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.stream.StreamSource;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.NamedElementHolder;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.ViewColumnType;

/**
 * Данные записи формы, состоящие из полей курсора и дополнительных полей.
 */
public class LyraFormData {
	private final NamedElementHolder<LyraFieldValue> fields = new NamedElementHolder<LyraFieldValue>() {

		@Override
		protected String getErrorMsg(String name) {
			return String.format(
					"Field '%s' is defined more than once in form data", name);
		}
	};
	private int recversion;
	private String formId;

	private SimpleDateFormat sdf;

	public LyraFormData() {

	}

	public LyraFormData(BasicCursor c, String formId) throws CelestaException,
			ParseException {
		if (c instanceof Cursor) {
			recversion = ((Cursor) c).getRecversion();
		}

		this.formId = formId;
		Object[] vals = c._currentValues();
		int i = 0;

		for (Entry<String, ?> column : c.meta().getColumns().entrySet()) {
			Object val = vals[i++];

			LyraFieldValue lfv;
			if (column.getValue() instanceof Column) {
				Column meta = (Column) column.getValue();
				lfv = LyraFieldValue.getValue(meta, val);
			} else {
				ViewColumnType meta = (ViewColumnType) column.getValue();
				lfv = LyraFieldValue.getValue(meta, column.getKey(), val);
			}
			fields.addElement(lfv);
		}
	}

	public LyraFormData(InputStream is) throws CelestaException {
		FormDataParser parser;
		parser = new FormDataParser();
		try {
			TransformerFactory.newInstance().newTransformer()
					.transform(new StreamSource(is), new SAXResult(parser));
		} catch (Exception e) {
			throw new CelestaException("XML deserialization error: %s",
					e.getMessage());
		}
	}

	/**
	 * Возвращает перечень полей.
	 */
	public Collection<LyraFieldValue> getFields() {
		return fields;
	}

	/**
	 * Передаёт значения в курсор.
	 * 
	 * @param c
	 *            Курсор.
	 * @throws CelestaException
	 *             ошибка Celesta
	 */
	public void populateFields(Cursor c) throws CelestaException {
		c.setRecversion(recversion);
		Iterator<LyraFieldValue> i = fields.iterator();
		while (i.hasNext()) {
			LyraFieldValue lfv = i.next();
			if (c.meta().getColumns().containsKey(lfv.getName())) {
				setCursorFieldValue(c, lfv);
			}
		}
	}

	private void setCursorFieldValue(Cursor c, LyraFieldValue lfv)
			throws CelestaException {
		Object val = lfv.getValue();
		if (val == null) {
			c.setValue(lfv.getName(), null);
		} else {
			String buf = val.toString();
			switch (lfv.getFieldType()) {
			case DATETIME:
				if (val instanceof Date)
					c.setValue(lfv.getName(), val);
				else {
					if (sdf == null)
						sdf = new SimpleDateFormat(
								LyraFieldValue.XML_DATE_FORMAT);

					Date d;
					try {
						d = sdf.parse(buf);
					} catch (java.text.ParseException e) {
						d = null;
					}
					c.setValue(lfv.getName(), d);
				}
				break;
			case BIT:
				c.setValue(lfv.getName(), Boolean.valueOf(buf));
				break;
			case INT:
				c.setValue(lfv.getName(), Integer.valueOf(buf));
				break;
			case REAL:
				c.setValue(lfv.getName(), Double.valueOf(buf));
				break;
			default:
				c.setValue(lfv.getName(), buf);
			}
		}
	}

	/**
	 * Добавляет значение поля в форму.
	 * 
	 * @param v
	 *            Значение поля.
	 * @throws ParseException
	 *             В случае, если поле с таким именем уже есть в форме.
	 */
	private void addFieldValue(LyraFieldValue v) throws ParseException {
		fields.addElement(v);
	}

	/**
	 * Добавляет поле с типом String.
	 * 
	 * @param name
	 *            имя
	 * @param value
	 *            значение
	 * @param local
	 *            локальная (не взятая из курсора) переменная
	 * @throws ParseException
	 *             неуникальное имя или неверное значение
	 */
	public void addValue(String name, String value, boolean local)
			throws ParseException {
		LyraFieldValue v = new LyraFieldValue(LyraFieldType.VARCHAR, name,
				value, local);
		addFieldValue(v);
	}

	/**
	 * Добавляет поле с типом int.
	 * 
	 * @param name
	 *            имя
	 * @param value
	 *            значение
	 * @param local
	 *            локальная (не взятая из курсора) переменная
	 * @throws ParseException
	 *             неуникальное имя или неверное значение
	 */
	public void addValue(String name, int value, boolean local)
			throws ParseException {
		LyraFieldValue v = new LyraFieldValue(LyraFieldType.INT, name, value,
				local);
		addFieldValue(v);
	}

	/**
	 * Добавляет поле с типом real.
	 * 
	 * @param name
	 *            имя
	 * @param value
	 *            значение
	 * @param local
	 *            локальная (не взятая из курсора) переменная
	 * @throws ParseException
	 *             неуникальное имя или неверное значение
	 */
	public void addValue(String name, double value, boolean local)
			throws ParseException {
		LyraFieldValue v = new LyraFieldValue(LyraFieldType.REAL, name, value,
				local);
		addFieldValue(v);
	}

	/**
	 * Добавляет поле с типом boolean.
	 * 
	 * @param name
	 *            имя
	 * @param value
	 *            значение
	 * @param local
	 *            локальная (не взятая из курсора) переменная
	 * @throws ParseException
	 *             неуникальное имя или неверное значение
	 */
	public void addValue(String name, boolean value, boolean local)
			throws ParseException {
		LyraFieldValue v = new LyraFieldValue(LyraFieldType.BIT, name, value,
				local);
		addFieldValue(v);
	}

	/**
	 * Добавляет поле с типом Date.
	 * 
	 * @param name
	 *            имя
	 * @param value
	 *            значение
	 * @param local
	 *            локальная (не взятая из курсора) переменная
	 * @throws ParseException
	 *             неуникальное имя или неверное значение
	 */
	public void addValue(String name, Date value, boolean local)
			throws ParseException {
		LyraFieldValue v = new LyraFieldValue(LyraFieldType.DATETIME, name,
				value, local);
		addFieldValue(v);
	}

	/**
	 * Устанавливает null-значение.
	 * 
	 * @param t
	 *            Тип поля
	 * @param name
	 *            имя поля
	 * @param local
	 *            локальная (не взятая из курсора) переменная
	 * @throws ParseException
	 *             неуникальное имя
	 * 
	 */
	public void addNullValue(LyraFieldType t, String name, boolean local)
			throws ParseException {
		LyraFieldValue v = new LyraFieldValue(t, name, null, local);
		addFieldValue(v);
	}

	/**
	 * Сериализует данные формы в поток в формате XML.
	 * 
	 * @param outputStream
	 *            Поток.
	 * @throws CelestaException
	 *             Ошибка записи в поток.
	 */
	public void serialize(OutputStream outputStream) throws CelestaException {
		try {
			XMLStreamWriter xmlWriter = XMLOutputFactory.newInstance()
					.createXMLStreamWriter(
							new OutputStreamWriter(outputStream, "UTF-8"));
			xmlWriter.writeStartDocument();
			xmlWriter.writeStartElement("schema");
			xmlWriter
					.writeAttribute("recversion", Integer.toString(recversion));
			if (formId != null)
				xmlWriter.writeAttribute("formId", formId);

			Iterator<LyraFieldValue> i = fields.iterator();
			while (i.hasNext()) {
				i.next().serialize(xmlWriter);
			}
			xmlWriter.writeEndDocument();
			xmlWriter.flush();
		} catch (Exception e) {
			throw new CelestaException("XML Serialization error: %s",
					e.getMessage());
		}
	}

	/**
	 * SAX-парсер сериализованного курсора.
	 */
	private final class FormDataParser extends DefaultHandler {

		private final StringBuilder sb = new StringBuilder();
		private String key;
		private int status = 0;
		private boolean isNull = false;
		private boolean local = false;
		private LyraFieldType type = null;

		@Override
		public void startElement(String uri, String localName, String qName,
				Attributes attributes) throws SAXException {
			switch (status) {
			case 0:
				recversion = Integer
						.parseInt(attributes.getValue("recversion"));
				formId = attributes.getValue("formId");
				status = 1;
				break;
			case 1:
				key = localName;
				isNull = Boolean.parseBoolean(attributes.getValue("null"));
				local = Boolean.parseBoolean(attributes.getValue("local"));
				type = LyraFieldType.valueOf(attributes.getValue("type"));
				status = 2;
				sb.setLength(0);
			default:
			}
		}

		@Override
		public void characters(char[] ch, int start, int length)
				throws SAXException {
			if (status == 2)
				sb.append(ch, start, length);
		}

		@Override
		public void endElement(String uri, String localName, String qName)
				throws SAXException {
			if (status == 2) {
				status = 1;
				try {
					if (isNull && sb.length() == 0) {
						addNullValue(type, key, local);
					} else {
						String buf = sb.toString();

						switch (type) {
						case DATETIME:
							if (sdf == null)
								sdf = new SimpleDateFormat(
										LyraFieldValue.XML_DATE_FORMAT);
							Date d;
							try {
								d = sdf.parse(buf);
							} catch (java.text.ParseException e) {
								d = null;
							}
							addValue(key, d, local);
							break;
						case BIT:
							addValue(key, Boolean.valueOf(buf), local);
							break;
						case INT:
							addValue(key, Integer.valueOf(buf), local);
							break;
						case REAL:
							addValue(key, Double.valueOf(buf), local);
							break;
						default:
							addValue(key, buf, local);
						}
					}
				} catch (ParseException e) {
					throw new SAXException(e.getMessage());
				}
			}
		}
	}
}
package ru.curs.lyra;

import static ru.curs.lyra.LyraFormField.REQUIRED;
import static ru.curs.lyra.LyraFormField.SCALE;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.stream.StreamSource;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.Cursor;

/**
 * A serializable cursor data represention.
 */
public final class LyraFormData implements Serializable {
	private static final long serialVersionUID = 1L;
	private final LyraNamedElementHolder<LyraFieldValue> fields = new LyraNamedElementHolder<LyraFieldValue>() {
		private static final long serialVersionUID = 1L;

		@Override
		protected String getErrorMsg(String name) {
			return "Field " + name + " is defined more than once in form data";
		}
	};
	private int recversion;

	private String formId;

	private SimpleDateFormat sdf;

	/**
	 * Creates a serializable cursor data representation.
	 * 
	 * @param c
	 *            A cursor.
	 * @param map
	 * @param formId
	 *            Fully qualified form class name.
	 * @throws CelestaException
	 *             names clash
	 */
	public LyraFormData(BasicCursor c, Map<String, LyraFormField> map, String formId) throws CelestaException {
		if (c instanceof Cursor) {
			recversion = ((Cursor) c).getRecversion();
		}

		this.formId = formId;
		Object[] vals = c._currentValues();

		for (LyraFormField lff : map.values()) {
			Object val = lff.getAccessor().getValue(vals);
			LyraFieldValue lfv = new LyraFieldValue(lff, val);
			fields.addElement(lfv);
		}
	}

	public LyraFormData(InputStream is) throws CelestaException {
		FormDataParser parser;
		parser = new FormDataParser();
		try {
			TransformerFactory.newInstance().newTransformer().transform(new StreamSource(is), new SAXResult(parser));
		} catch (Exception e) {
			throw new CelestaException("XML deserialization error: %s", e.getMessage());
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
	 * @param map
	 *            Набор полей.
	 * @throws CelestaException
	 *             ошибка Celesta
	 */
	public void populateFields(Cursor c, Map<String, LyraFormField> map) throws CelestaException {
		c.setRecversion(recversion);
		for (LyraFieldValue lfv : fields) {
			LyraFormField lff = map.get(lfv.getName());
			if (lff != null) {
				lff.getAccessor().setValue(c, lfv.getValue());
			}
		}
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
					.createXMLStreamWriter(new OutputStreamWriter(outputStream, "UTF-8"));
			xmlWriter.writeStartDocument();
			xmlWriter.writeStartElement("schema");
			xmlWriter.writeAttribute("recversion", Integer.toString(recversion));
			if (formId != null)
				xmlWriter.writeAttribute("formId", formId);

			Iterator<LyraFieldValue> i = fields.iterator();
			while (i.hasNext()) {
				i.next().serialize(xmlWriter);
			}
			xmlWriter.writeEndDocument();
			xmlWriter.flush();
		} catch (Exception e) {
			throw new CelestaException("XML Serialization error: %s", e.getMessage());
		}
	}

	/**
	 * Recversion of serialized data.
	 */
	public int getRecversion() {
		return recversion;
	}

	/**
	 * SAX-парсер сериализованного курсора.
	 */
	private final class FormDataParser extends DefaultHandler {

		private final StringBuilder sb = new StringBuilder();
		private String key;
		private int status = 0;
		private boolean isNull = false;
		private LyraFieldType type = null;
		private int scale;
		private boolean required = false;

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes)
				throws SAXException {
			switch (status) {
			case 0:
				recversion = Integer.parseInt(attributes.getValue("recversion"));
				formId = attributes.getValue("formId");
				status = 1;
				break;
			case 1:
				key = localName;
				type = LyraFieldType.valueOf(attributes.getValue("type"));

				String buf = attributes.getValue("null");
				isNull = buf == null ? false : Boolean.parseBoolean(buf);

				buf = attributes.getValue(SCALE);
				scale = buf == null ? LyraFormField.DEFAULT_SCALE : Integer.parseInt(buf);

				buf = attributes.getValue(REQUIRED);
				required = buf == null ? false : Boolean.parseBoolean(buf);

				status = 2;
				sb.setLength(0);
			default:
			}
		}

		@Override
		public void characters(char[] ch, int start, int length) throws SAXException {
			if (status == 2)
				sb.append(ch, start, length);
		}

		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {
			if (status == 2) {
				status = 1;
				try {
					if (isNull && sb.length() == 0) {
						addNullValue(type, key);
					} else {
						String buf = sb.toString();
						LyraFormField lff = new LyraFormField(key);
						lff.setScale(scale);
						lff.setRequired(required);
						switch (type) {
						case DATETIME:
							if (sdf == null)
								sdf = new SimpleDateFormat(LyraFieldValue.XML_DATE_FORMAT);
							Date d;
							try {
								d = sdf.parse(buf);
							} catch (java.text.ParseException e) {
								d = null;
							}
							addValue(lff, d);
							break;
						case BIT:
							addValue(lff, Boolean.valueOf(buf));
							break;
						case INT:
							addValue(lff, Integer.valueOf(buf));
							break;
						case REAL:
							addValue(lff, Double.valueOf(buf));
							break;
						default:
							addValue(lff, buf);
						}
					}
				} catch (CelestaException e) {
					throw new SAXException(e.getMessage());
				}
			}
		}

		private void addFieldValue(LyraFieldValue v) throws CelestaException {
			fields.addElement(v);
		}

		private void addValue(LyraFormField lff, String value) throws CelestaException {
			lff.setType(LyraFieldType.VARCHAR);
			LyraFieldValue v = new LyraFieldValue(lff, value);
			addFieldValue(v);
		}

		private void addValue(LyraFormField lff, Integer value) throws CelestaException {
			lff.setType(LyraFieldType.INT);
			LyraFieldValue v = new LyraFieldValue(lff, value);
			addFieldValue(v);
		}

		private void addValue(LyraFormField lff, Double value) throws CelestaException {
			lff.setType(LyraFieldType.REAL);
			LyraFieldValue v = new LyraFieldValue(lff, value);
			addFieldValue(v);
		}

		private void addValue(LyraFormField lff, Boolean value) throws CelestaException {
			lff.setType(LyraFieldType.BIT);
			LyraFieldValue v = new LyraFieldValue(lff, value);
			addFieldValue(v);
		}

		private void addValue(LyraFormField lff, Date value) throws CelestaException {
			lff.setType(LyraFieldType.DATETIME);
			LyraFieldValue v = new LyraFieldValue(lff, value);
			addFieldValue(v);
		}

		private void addNullValue(LyraFieldType t, String name) throws CelestaException {
			LyraFormField lff = new LyraFormField(name);
			lff.setScale(scale);
			lff.setRequired(required);
			lff.setType(t);
			LyraFieldValue v = new LyraFieldValue(lff, null);
			addFieldValue(v);
		}
	}
}
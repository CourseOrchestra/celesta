package ru.curs.lyra;

import static ru.curs.lyra.LyraFormField.*;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

import javax.xml.stream.*;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.stream.StreamSource;

import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.*;

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
	private Object[] keyValues;

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
			keyValues = ((Cursor) c).getCurrentKeyValues();
		} else {
			// TODO: here we have an assumption that the first field is the key
			// field
			keyValues = new Object[1];
			keyValues[0] = c._currentValues()[0];
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
	 * Key values of serialized data.
	 */
	public Object[] getKeyValues() {
		return keyValues;
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
		private String subtype = null;

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

				buf = attributes.getValue(SUBTYPE);
				subtype = buf;

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
					LyraFieldValue v;
					LyraFormField lff = new LyraFormField(key);
					lff.setScale(scale);
					lff.setRequired(required);
					lff.setType(type);
					lff.setSubtype(subtype);
					if (isNull && sb.length() == 0) {
						v = new LyraFieldValue(lff, null);
					} else {
						String buf = sb.toString();
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
							v = new LyraFieldValue(lff, d);
							break;
						case BIT:
							v = new LyraFieldValue(lff, Boolean.valueOf(buf));
							break;
						case INT:
							v = new LyraFieldValue(lff, Integer.valueOf(buf));
							break;
						case REAL:
							v = new LyraFieldValue(lff, Double.valueOf(buf));
							break;
						default:
							v = new LyraFieldValue(lff, buf);
						}
					}
					fields.addElement(v);
				} catch (CelestaException e) {
					throw new SAXException(e.getMessage());
				}
			}
		}

	}
}
package ru.curs.lyra;

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
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.ColumnMeta;
import ru.curs.celesta.score.ViewColumnType;

/**
 * A serializable cursor data represention.
 */
final class LyraFormData implements Serializable {
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
			LyraFieldValue lfv;
			if (lff.isBound()) {
				ColumnMeta cmeta = c.meta().getColumns().get(lff.getName());

				if (cmeta == null) {
					throw new CelestaException("Column %s does not exists in '%s'.", lff.getName(), c.meta().getName());
				} else if (cmeta instanceof Column) {
					Column meta = (Column) cmeta;
					lfv = LyraFieldValue.getValue(meta, val);
				} else {
					ViewColumnType meta = (ViewColumnType) cmeta;
					lfv = LyraFieldValue.getValue(meta, lff.getName(), val);
				}
			} else {
				lfv = new LyraFieldValue(lff.getType(), lff.getName(), lff.getAccessor().getValue(vals), true);
			}
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
				isNull = Boolean.parseBoolean(attributes.getValue("null"));
				local = Boolean.parseBoolean(attributes.getValue("local"));
				type = LyraFieldType.valueOf(attributes.getValue("type"));
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
						addNullValue(type, key, local);
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
				} catch (CelestaException e) {
					throw new SAXException(e.getMessage());
				}
			}
		}

		private void addFieldValue(LyraFieldValue v) throws CelestaException {
			fields.addElement(v);
		}

		private void addValue(String name, String value, boolean local) throws CelestaException {
			LyraFieldValue v = new LyraFieldValue(LyraFieldType.VARCHAR, name, value, local);
			addFieldValue(v);
		}

		private void addValue(String name, int value, boolean local) throws CelestaException {
			LyraFieldValue v = new LyraFieldValue(LyraFieldType.INT, name, value, local);
			addFieldValue(v);
		}

		private void addValue(String name, double value, boolean local) throws CelestaException {
			LyraFieldValue v = new LyraFieldValue(LyraFieldType.REAL, name, value, local);
			addFieldValue(v);
		}

		private void addValue(String name, boolean value, boolean local) throws CelestaException {
			LyraFieldValue v = new LyraFieldValue(LyraFieldType.BIT, name, value, local);
			addFieldValue(v);
		}

		private void addValue(String name, Date value, boolean local) throws CelestaException {
			LyraFieldValue v = new LyraFieldValue(LyraFieldType.DATETIME, name, value, local);
			addFieldValue(v);
		}

		private void addNullValue(LyraFieldType t, String name, boolean local) throws CelestaException {
			LyraFieldValue v = new LyraFieldValue(t, name, null, local);
			addFieldValue(v);
		}
	}
}
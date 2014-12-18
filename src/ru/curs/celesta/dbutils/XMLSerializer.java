package ru.curs.celesta.dbutils;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
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

/**
 * Класс для сериализации и десериализации содержимого курсора в XML.
 */
public final class XMLSerializer {

	private XMLSerializer() {

	}

	/**
	 * Десериализует курсор из XML.
	 * 
	 * @param c
	 *            Курсор.
	 * @param inputStream
	 *            Поток, из которого читается курсор.
	 * @throws CelestaException
	 *             Ошибка формата или чтения потока.
	 */
	public static void deserialize(Cursor c, InputStream inputStream)
			throws CelestaException {

		FormDataParser parser;

		parser = new FormDataParser(c);

		try {
			TransformerFactory
					.newInstance()
					.newTransformer()
					.transform(new StreamSource(inputStream),
							new SAXResult(parser));
		} catch (Exception e) {
			throw new CelestaException("XML deserialization error: %s",
					e.getMessage());
		}
	}

	/**
	 * Сериализует курсор в поток в формате XML.
	 * 
	 * @param c
	 *            Курсор.
	 * @param outputStream
	 *            Поток.
	 * @throws CelestaException
	 *             Ошибка записи в поток.
	 */
	public static void serialize(Cursor c, OutputStream outputStream)
			throws CelestaException {
		try {
			XMLStreamWriter xmlWriter = XMLOutputFactory.newInstance()
					.createXMLStreamWriter(
							new OutputStreamWriter(outputStream, "UTF-8"));
			xmlWriter.writeStartDocument();
			xmlWriter.writeStartElement("data");
			xmlWriter.writeAttribute("recversion",
					Integer.toString(c.getRecversion()));

			Object[] vals = c._currentValues();
			int i = 0;
			for (Entry<String, Column> column : c.meta().getColumns()
					.entrySet()) {
				Object val = vals[i++];
				xmlWriter.writeStartElement(column.getKey());
				xmlWriter.writeAttribute("type", column.getValue()
						.getCelestaType());
				xmlWriter.writeAttribute("null", Boolean.toString(val == null));
				xmlWriter.writeCharacters(val == null ? "" : val.toString());
				xmlWriter.writeEndElement();
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
	private static final class FormDataParser extends DefaultHandler {
		private final Cursor c;
		private final StringBuilder sb = new StringBuilder();
		private String key;
		private int status = 0;
		private boolean isNull = false;

		FormDataParser(Cursor c) {
			this.c = c;

		}

		@Override
		public void startElement(String uri, String localName, String qName,
				Attributes attributes) throws SAXException {
			switch (status) {
			case 0:
				int recversion = Integer.parseInt(attributes
						.getValue("recversion"));
				c.setRecversion(recversion);
				status = 1;
				break;
			case 1:
				key = localName;
				isNull = Boolean.parseBoolean(attributes.getValue("null"));
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
			if (status == 2)
				status = 1;
			try {
				if (isNull && sb.length() == 0) {
					c.setValue(key, null);
				} else {

					c.setValue(key, sb.toString());
				}
			} catch (CelestaException e) {
				throw new SAXException(e.getMessage());
			}
		}
	}
}

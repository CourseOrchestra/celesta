package dbschemasync;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Iterator;
import java.util.Random;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.ForeignKey;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.Index;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.StringColumn;
import ru.curs.celesta.score.Table;

/**
 * Класс-преобразователь Score в DBS-файл.
 */
public final class Celesta2DBSchema {

	private Celesta2DBSchema() {

	}

	/**
	 * Переводит score в DBS-файл.
	 * 
	 * @param s
	 *            Celesta score.
	 * @param dbsFile
	 *            Файл DBSChema.
	 * @throws Exception
	 *             Любая ошибка.
	 */
	public static void scoreToDBS(Score s, File dbsFile) throws Exception {

		DocumentBuilderFactory docFactory = DocumentBuilderFactory
				.newInstance();
		DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
		Document doc;
		if (!dbsFile.exists()) {
			FileOutputStream fos = new FileOutputStream(dbsFile);
			try {
				XMLStreamWriter sw = XMLOutputFactory.newFactory()
						.createXMLStreamWriter(fos, "utf-8");
				sw.writeStartDocument();
				sw.writeStartElement("project");
				sw.writeAttribute("name", "CelestaReversed");
				sw.writeAttribute("database", "Celesta");
				sw.writeAttribute("id",
						String.format("Project%d", (new Random()).nextInt()));
				sw.writeStartElement("layout");
				sw.writeAttribute("id",
						String.format("Layout%d", (new Random()).nextInt()));
				sw.writeAttribute("name", "celesta");
				sw.writeEndElement();
				sw.writeEndElement();
				sw.writeEndDocument();
				sw.flush();
			} finally {
				fos.close();
			}
		}
		doc = docBuilder.parse(dbsFile);
		Element root = doc.getDocumentElement();

		NodeList l = root.getChildNodes();
		// Находим первое упоминание layout: таблицы надо вставить до него,
		// иначе некорректно прочитается файл проекта.
		Node layout = null;
		for (int i = 0; i < l.getLength(); i++) {
			Node n = l.item(i);
			if ("schema".equals(n.getNodeName()))
				root.removeChild(n);
			else if ("layout".equals(n.getNodeName()) && layout == null)
				layout = n;
		}

		for (Grain g : s.getGrains().values()) {
			Element schema = doc.createElement("schema");
			schema.setAttribute("name", g.getName());
			schema.setAttribute("schemaname", g.getName());
			schema.setAttribute("defo", "y");
			root.insertBefore(schema, layout);
			writeComment(g.getCelestaDoc(), doc, schema);

			for (Table t : g.getTables().values())
				writeTable(g, t, doc, schema);

			Element procedure = doc.createElement("procedure");
			procedure.setAttribute("name", g.getName());
			procedure.setAttribute("isSystem", "false");
			schema.appendChild(procedure);
			Element string = doc.createElement("string");
			string.setTextContent(String.format(
					"create grain %s version '%s';", g.getName(), g
							.getVersion().toString()));
			procedure.appendChild(string);
		}
		TransformerFactory transformerFactory = TransformerFactory
				.newInstance();
		Transformer transformer = transformerFactory.newTransformer();
		transformer.setOutputProperty(OutputKeys.METHOD, "xml");
		transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
		transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		DOMSource source = new DOMSource(doc);
		StreamResult result = new StreamResult(dbsFile);
		transformer.transform(source, result);

	}

	private static void writeColumn(Column c, Document doc, Element table) {
		Element column = doc.createElement("column");
		column.setAttribute("name", c.getName());
		column.setAttribute("type", c.getCelestaType());
		if (c instanceof StringColumn) {
			StringColumn sc = (StringColumn) c;
			if (sc.isMax()) {
				column.setAttribute("type", c.getCelestaType() + "(MAX)");
			} else {
				column.setAttribute("length", Integer.toString(sc.getLength()));
			}
		} else if (c instanceof IntegerColumn) {
			IntegerColumn ic = (IntegerColumn) c;
			if (ic.isIdentity())
				column.setAttribute("autoincrement", "y");
		}
		if (!c.isNullable())
			column.setAttribute("mandatory", "y");

		if (c.getCelestaDefault() != null) {
			Element defo = doc.createElement("defo");
			defo.setTextContent(c.getCelestaDefault());
			column.appendChild(defo);
		}
		writeComment(c.getCelestaDoc(), doc, column);
		table.appendChild(column);
	}

	private static void writeComment(String celestaDoc, Document doc,
			Element parent) {
		if (celestaDoc != null) {
			Element comment = doc.createElement("comment");
			comment.appendChild(doc.createCDATASection(celestaDoc));
			parent.appendChild(comment);
		}
	}

	private static void writeTable(Grain g, Table t, Document doc,
			Element schema) {
		Element table = doc.createElement("table");
		table.setAttribute("name", t.getName());
		schema.appendChild(table);
		writeComment(t.getCelestaDoc(), doc, table);

		// Writing columns
		for (Column c : t.getColumns().values())
			writeColumn(c, doc, table);

		// Writing primary key
		Element index = doc.createElement("index");
		index.setAttribute("name", t.getPkConstraintName());
		index.setAttribute("unique", "PRIMARY_KEY");
		table.appendChild(index);
		for (Column c : t.getPrimaryKey().values()) {
			Element column = doc.createElement("column");
			column.setAttribute("name", c.getName());
			index.appendChild(column);
		}

		// Writing foreign keys
		for (ForeignKey fk : t.getForeignKeys()) {
			Element efk = doc.createElement("fk");
			table.appendChild(efk);
			efk.setAttribute("name", fk.getConstraintName());
			efk.setAttribute("to_schema", fk.getReferencedTable().getGrain()
					.getName());
			efk.setAttribute("to_table", fk.getReferencedTable().getName());
			switch (fk.getDeleteRule()) {
			case CASCADE:
				efk.setAttribute("delete_action", "cascade");
				break;
			case SET_NULL:
				efk.setAttribute("delete_action", "setNull");
				break;
			default:
			}

			switch (fk.getUpdateRule()) {
			case CASCADE:
				efk.setAttribute("update_action", "cascade");
				break;
			case SET_NULL:
				efk.setAttribute("update_action", "setNull");
				break;
			default:
			}

			Iterator<Column> i = fk.getReferencedTable().getPrimaryKey()
					.values().iterator();
			for (Column c : fk.getColumns().values()) {
				Element fkColumn = doc.createElement("fk_column");
				efk.appendChild(fkColumn);
				fkColumn.setAttribute("name", c.getName());
				fkColumn.setAttribute("pk", i.next().getName());
			}
		}

		// Writing indices
		for (Index ix : g.getIndices().values())
			if (ix.getTable() == t) {
				index = doc.createElement("index");
				index.setAttribute("name", ix.getName());
				index.setAttribute("unique", "NORMAL");
				table.appendChild(index);
				writeComment(ix.getCelestaDoc(), doc, index);
				for (Column c : ix.getColumns().values()) {
					Element column = doc.createElement("column");
					column.setAttribute("name", c.getName());
					index.appendChild(column);
				}
			}
	}

}

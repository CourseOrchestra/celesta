package ru.curs.celesta.dbschemasync;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;

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
import ru.curs.celesta.score.Namespace;
import ru.curs.celesta.score.AbstractScore;
import ru.curs.celesta.score.CelestaSerializer;
import ru.curs.celesta.score.StringColumn;
import ru.curs.celesta.score.Table;
import ru.curs.celesta.score.View;
import ru.curs.celesta.score.ViewColumnMeta;

/**
 * Converter class: Score to DBS file.
 */
public final class Celesta2DBSchema {

    private Celesta2DBSchema() {

    }

    /**
     * Converts score to DBS file.
     *
     * @param s  Celesta score
     * @param dbsFile  DBSchema file
     * @throws Exception  any error
     */
    public static void scoreToDBS(AbstractScore s, File dbsFile) throws Exception {

        if (!dbsFile.exists()) {
            try (FileOutputStream fos = new FileOutputStream(dbsFile)) {
                XMLStreamWriter sw = XMLOutputFactory.newInstance().createXMLStreamWriter(fos, "utf-8");
                sw.writeStartDocument();
                sw.writeStartElement("project");
                sw.writeAttribute("name", "CelestaReversed");
                sw.writeAttribute("database", "Celesta");
                Random r = ThreadLocalRandom.current();
                sw.writeAttribute("id", String.format("Project%d", r.nextInt()));
                sw.writeStartElement("layout");
                sw.writeAttribute("id", String.format("Layout%d", r.nextInt()));
                sw.writeAttribute("name", "celesta");
                sw.writeEndElement();
                sw.writeEndElement();
                sw.writeEndDocument();
            }
        }

        DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
        Document doc = docBuilder.parse(dbsFile);
        Element root = doc.getDocumentElement();

        NodeList l = root.getChildNodes();
        // Find the first mentioning of layout: tables should be inserted before it,
        // otherwise the project file would be read incorrectly.
        Node layout = null;
        for (int i = 0; i < l.getLength(); i++) {
            Node n = l.item(i);
            if ("schema".equals(n.getNodeName())) {
                root.removeChild(n);
            } else if ("layout".equals(n.getNodeName()) && layout == null) {
                layout = n;
            }
        }

        for (Grain g : s.getGrains().values()) {
            Element schema = doc.createElement("schema");
            final String schemaName = Optional.of(g.getNamespace())
                                              .filter(Predicate.isEqual(Namespace.DEFAULT).negate())
                                              .map(ns -> ns.getValue() + "." + g.getName())
                                              .orElse(g.getName());
            schema.setAttribute("name", schemaName);
            schema.setAttribute("schemaname", schemaName);
            schema.setAttribute("defo", "y");
            root.insertBefore(schema, layout);
            writeComment(g.getCelestaDoc(), doc, schema);

            for (Table t : g.getTables().values()) {
                writeTable(g, t, doc, schema);
            }

            for (View v : g.getViews().values()) {
                writeView(v, doc, schema);
            }

            Element procedure = doc.createElement("procedure");
            procedure.setAttribute("name", g.getName());
            procedure.setAttribute("isSystem", "false");
            schema.appendChild(procedure);
            Element string = doc.createElement("string");
            string.setTextContent(
                    String.format("create grain %s version '%s';", g.getName(), g.getVersion().toString()));
            procedure.appendChild(string);
        }

        // TODO:
        // Schema name is included as name attribute of schema node to be shown in DbSchema.
        // This may however break existing layouts. As a workaround it is suggested to re-map layout.entity.schema
        // attributes f.e:
        // if layout.entity.schema ~= "*.log" { layout.entity.schema = "new.namespace.log" }

        TransformerFactory transformerFactory = TransformerFactory.newInstance();
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
                column.setAttribute("type", c.getCelestaType());
            } else {
                column.setAttribute("length", Integer.toString(sc.getLength()));
            }
        }
        if (!c.isNullable()) {
            column.setAttribute("mandatory", "y");
        }

        String def = c.getCelestaDefault();
        if (def != null) {
            Element defo = doc.createElement("defo");
            if ("GETDATE()".equalsIgnoreCase(def)) {
                def = "GETDATE";
            }
            defo.setTextContent(def);
            column.appendChild(defo);
        }
        writeComment(c.getCelestaDoc(), doc, column);
        table.appendChild(column);
    }

    private static void writeComment(String celestaDoc, Document doc, Element parent) {
        if (celestaDoc != null) {
            Element comment = doc.createElement("comment");
            //Seems to be a bug in XML Builder
            // that substitute CRLF->CRCRLF and this doubles spaces between lines in Celstadoc
            comment.appendChild(doc.createCDATASection(celestaDoc.replaceAll("\\r\\n", "\n")));
            parent.appendChild(comment);
        }
    }

    private static void writeTable(Grain g, Table t, Document doc, Element schema) {
        Element table = doc.createElement("table");
        table.setAttribute("name", t.getName());
        schema.appendChild(table);
        writeComment(t.getCelestaDoc(), doc, table);

        // Writing columns
        for (Column c : t.getColumns().values()) {
            writeColumn(c, doc, table);
        }

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
            efk.setAttribute("to_schema", fk.getReferencedTable().getGrain().getName());
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

            Iterator<Column> i = fk.getReferencedTable().getPrimaryKey().values().iterator();
            for (Column c : fk.getColumns().values()) {
                Element fkColumn = doc.createElement("fk_column");
                efk.appendChild(fkColumn);
                fkColumn.setAttribute("name", c.getName());
                fkColumn.setAttribute("pk", i.next().getName());
            }
        }

        // Writing indices
        for (Index ix : g.getIndices().values()) {
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

        // Writing storage options
        writeOptions(t, doc, table);
    }

    private static void writeView(View v, Document doc, Element schema) throws IOException {
        Element view = doc.createElement("view");
        view.setAttribute("name", v.getName());
        schema.appendChild(view);
        writeComment(v.getCelestaDoc(), doc, view);
        Element viewScript = doc.createElement("view_script");
        viewScript.appendChild(doc.createCDATASection(CelestaSerializer.toQueryString(v)));
        view.appendChild(viewScript);

        // Writing columns
        for (Map.Entry<String, ViewColumnMeta> c : v.getColumns().entrySet()) {
            writeColumn(c, doc, view);
        }
    }

    private static void writeColumn(Map.Entry<String, ViewColumnMeta> c, Document doc, Element view) {
        Element column = doc.createElement("column");
        column.setAttribute("name", c.getKey());
        column.setAttribute("type", c.getValue().getCelestaType());
        view.appendChild(column);
    }

    private static void writeOptions(Table t, Document doc, Element table) {
        Element storage = doc.createElement("storage");
        String options;
        if (t.isVersioned()) {
            options = "WITH VERSION CHECK";
        } else {
            if (t.isReadOnly()) {
                options = "WITH READ ONLY";
            } else {
                options = "WITH NO VERSION CHECK";
            }
        }
        if (!t.isAutoUpdate()) {
            options += " NO AUTOUPDATE";
        }
        storage.appendChild(doc.createCDATASection(options));
        table.appendChild(storage);
    }

}

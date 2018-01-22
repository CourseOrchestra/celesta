package ru.curs.celesta.dbschemasync;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.BinaryColumn;
import ru.curs.celesta.score.BooleanColumn;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.DateTimeColumn;
import ru.curs.celesta.score.FKRule;
import ru.curs.celesta.score.FloatingColumn;
import ru.curs.celesta.score.ForeignKey;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.Index;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.StringColumn;
import ru.curs.celesta.score.Table;
import ru.curs.celesta.score.View;
import ru.curs.celesta.score.ViewColumnMeta;

/**
 * Переносит данные из DBSchema в Celesta.
 */
public final class DBSchema2Celesta {

    private static final Pattern VERSION = Pattern.compile("version *('[^']+') *;", Pattern.CASE_INSENSITIVE);

    private DBSchema2Celesta() {
    }

    /**
     * Преобразует DBS в Score.
     *
     * @param dbs          DBS-файл
     * @param refScore     score.
     * @param withPlantUml также вывести PlantUml-диаграммы для каждого из View.
     * @throws Exception любая ошибка.
     */
    public static void dBSToScore(File dbs, Score refScore, boolean withPlantUml) throws Exception {
        DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
        Document doc;
        doc = docBuilder.parse(dbs);
        Element root = doc.getDocumentElement();
        NodeList l = root.getChildNodes();

        for (int i = 0; i < l.getLength(); i++) {
            Node n = l.item(i);
            if ("schema".equals(n.getNodeName())) {
                Element schema = (Element) n;
                String grainName = schema.getAttribute("name");

                if (grainName == null || grainName.isEmpty()) {
                    throw new Exception("Empty schema name found.");
                } else if ("celesta".equals(grainName)) {
                    // Схему celesta не трогаем!
                    continue;
                }
                Grain g = refScore.getGrains().get(grainName);
                if (g == null)
                    g = new Grain(refScore, schema.getAttribute("name"));
                updateGrain(schema, g);
                g.finalizeParsing();
            }
        }

        // Вторым проходом отдельно обновляем внешние ключи на всех таблицах.
        for (int i = 0; i < l.getLength(); i++) {
            Node n = l.item(i);
            if ("schema".equals(n.getNodeName())) {
                Element schema = (Element) n;
                String grainName = schema.getAttribute("name");
                if (grainName == null || grainName.isEmpty()) {
                    throw new Exception("Empty schema name found.");
                } else if ("celesta".equals(grainName)) {
                    // Схему celesta не трогаем!
                    continue;
                }
                Grain g = refScore.getGrains().get(grainName);
                updateGrainFK(schema, g);
                // Only to raise "modified" flag
                g.setVersion("'" + g.getVersion().toString() + "'");
            }
        }

        plantUml(withPlantUml, dbs, refScore, l);

        refScore.save();

    }

    private static void plantUml(boolean withPlantUml, File dbs, Score refScore, NodeList l) throws CelestaException {
        if (withPlantUml) {
            for (int i = 0; i < l.getLength(); i++) {
                Node n = l.item(i);
                if ("layout".equals(n.getNodeName())) {
                    Element layout = (Element) n;
                    writeADoc(dbs, layout, refScore);
                }
            }
        }
    }

    private static void writeADoc(File dbs, Element layout, Score refScore) throws CelestaException {
        String viewName = layout.getAttribute("name");
        File docFile = new File(dbs.getAbsoluteFile().getParentFile().getAbsolutePath(),
                String.format("%s.adoc", viewName));
        try {
            try (PrintWriter bw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(docFile), "utf-8"))) {
                bw.printf("[uml,file=\"%s.png\"]%n", viewName);
                bw.println("--");
                bw.println("@startuml");
                bw.println();
                bw.println("skinparam monochrome true");
                bw.println("skinparam dpi 150");
                bw.println();
                NodeList l = layout.getChildNodes();
                Set<Table> tables = new HashSet<>();
                for (int i = 0; i < l.getLength(); i++) {
                    Node n = l.item(i);
                    if ("entity".equals(n.getNodeName())) {
                        Element entity = (Element) n;
                        Grain g = refScore.getGrain(entity.getAttribute("schema"));
                        String eName = entity.getAttribute("name");
                        Table t = g.getTables().get(eName);
                        if (t != null) {
                            bw.printf("class %s {%n", t.getName());
                            for (Entry<String, Column> c : t.getColumns().entrySet()) {
                                bw.printf("  %s: %s%n", c.getKey(), c.getValue().getCelestaType());
                            }
                            bw.println("}");
                            bw.println();
                            tables.add(t);
                        } else {
                            View v = g.getViews().get(eName);
                            if (v != null) {
                                bw.printf("class %s <<view>>{%n", v.getName());
                                for (Entry<String, ViewColumnMeta> c : v.getColumns().entrySet()) {
                                    bw.write(String.format("  %s: %s%n", c.getKey(), c.getValue().getCelestaType()));
                                }
                                bw.println("}");
                                bw.println();
                            }
                        }
                    }
                }
                // Добавляем ссылки между таблицами, присутствующими на
                // диаграмме
                for (Table t : tables)
                    for (ForeignKey fk : t.getForeignKeys()) {
                        Table refTable = fk.getReferencedTable();

                        String columns = fk.getColumns().size() == 1 ? fk.getColumns().keySet().iterator().next()
                                : fk.getColumns().keySet().toString();

                        if (tables.contains(refTable))
                            bw.write(String.format("%s --> %s: %s %n%n", fk.getParentTable().getName(),
                                    fk.getReferencedTable().getName(), columns));
                    }

                bw.println("@enduml");
                bw.println("--");
            }

        } catch (IOException | ParseException e) {
            throw new CelestaException("Cannot save '%s': %s", docFile.getName(), e.getMessage());
        }

    }

    private static void updateGrainFK(Element schema, Grain g) throws ParseException {
        NodeList l = schema.getChildNodes();
        for (int i = 0; i < l.getLength(); i++) {
            Node n = l.item(i);
            if ("table".equals(n.getNodeName())) {
                Element table = (Element) n;
                Table t = g.getTable(table.getAttribute("name"));
                updateTableFK(table, t);
            }
        }
    }

    private static void updateGrain(Element schema, Grain g) throws Exception {
        // Зачищаем старый score.
        List<Index> indices = new ArrayList<>(g.getIndices().values());
        for (Index i : indices)
            i.delete();
        List<Table> tables = new ArrayList<>(g.getTables().values());
        for (Table t : tables)
            t.delete();
        List<View> views = new ArrayList<>(g.getViews().values());
        for (View v : views)
            v.delete();

        NodeList l = schema.getChildNodes();
        for (int i = 0; i < l.getLength(); i++) {
            Node n = l.item(i);
            if ("procedure".equals(n.getNodeName())) {
                Element procedure = (Element) n;
                if (g.getName().equals(procedure.getAttribute("name"))) {
                    String proc = procedure.getTextContent();
                    Matcher m = VERSION.matcher(proc);
                    if (m.find()) {
                        g.setVersion(m.group(1));
                    }
                }
            } else if ("comment".equals(n.getNodeName())) {
                g.setCelestaDoc(extractComment((Element) n));
            } else if ("table".equals(n.getNodeName())) {
                Element table = (Element) n;
                Table t = new Table(g, table.getAttribute("name"));
                updateTable(table, t);
            } else if ("view".equals(n.getNodeName())) {
                Element view = (Element) n;
                createView(g, view);
            }
        }
    }

    private static void createView(Grain g, Element view) throws ParseException {
        NodeList vl = view.getChildNodes();
        View cview = null;
        String celestaDoc = null;
        for (int i = 0; i < vl.getLength(); i++) {
            Node vn = vl.item(i);
            if ("view_script".equals(vn.getNodeName())) {
                Element viewScript = (Element) vn;
                String sql = viewScript.getTextContent().trim();
                cview = new View(g, view.getAttribute("name"), sql);
            } else if ("comment".equals(vn.getNodeName())) {
                celestaDoc = extractComment((Element) vn);
            }
        }
        if (cview != null)
            cview.setCelestaDoc(celestaDoc);
    }

    private static void updateTableFK(Element table, Table t) throws ParseException {
        NodeList l = table.getChildNodes();
        for (int i = 0; i < l.getLength(); i++) {
            Node n = l.item(i);
            if ("fk".equals(n.getNodeName())) {
                Element fk = (Element) n;
                updateFK(fk, t);
            }
        }
    }

    private static String extractComment(Element comment) {
        return comment.getTextContent().trim();
    }

    private static void updateTable(Element table, Table t) throws Exception {
        NodeList l = table.getChildNodes();
        List<String> options = Collections.emptyList();
        for (int i = 0; i < l.getLength(); i++) {
            Node n = l.item(i);
            if ("comment".equals(n.getNodeName())) {
                t.setCelestaDoc(extractComment((Element) n));
            } else if ("column".equals(n.getNodeName())) {
                Element column = (Element) n;
                updateColumn(column, t);
            } else if ("index".equals(n.getNodeName())) {
                Element index = (Element) n;
                updateIndex(index, t);
            } else if ("storage".equals(n.getNodeName())) {
                Element storage = (Element) n;
                options = Arrays.asList(extractComment(storage).split("\\s+"));
            }
        }
        parseOptions(options, t);
        t.finalizePK();
    }

    /**
     * Финализирует таблицу с перечнем WITH-опций.
     *
     * @param options Перечень опций.
     * @throws ParseException Ошибка определения таблицы.
     */
    // CHECKSTYLE:OFF for cyclomatic complexity: this is finite state machine
    private static void parseOptions(List<String> options, Table t) throws ParseException {
        // CHECKSTYLE:ON
        int state = 0;
        for (String option : options)
            switch (state) {
                // beginning
                case 0:
                    if ("with".equalsIgnoreCase(option)) {
                        state = 1;
                    } else {
                        throwPE(option, t.getName());
                    }
                    break;
                // 'with' read
                case 1:
                    if ("read".equalsIgnoreCase(option)) {
                        t.setVersioned(false);
                        t.setReadOnly(true);
                        state = 2;
                    } else if ("version".equalsIgnoreCase(option)) {
                        t.setReadOnly(false);
                        t.setVersioned(true);
                        state = 3;
                    } else if ("no".equalsIgnoreCase(option)) {
                        state = 4;
                    } else {
                        throwPE(option, t.getName());
                    }
                    break;
                case 2:
                    if ("only".equalsIgnoreCase(option)) {
                        state = 5;
                    } else {
                        throwPE(option, t.getName());
                    }
                    break;
                case 3:
                    if ("check".equalsIgnoreCase(option)) {
                        state = 5;
                    } else {
                        throwPE(option, t.getName());
                    }
                    break;
                case 4:
                    // 'no' read for the first time
                    if ("version".equalsIgnoreCase(option)) {
                        state = 3;
                        t.setReadOnly(false);
                        t.setVersioned(false);
                    } else if ("autoupdate".equalsIgnoreCase(option)) {
                        state = 7;
                        t.setAutoUpdate(false);
                    } else {
                        throwPE(option, t.getName());
                    }
                    break;
                case 5:
                    if ("no".equalsIgnoreCase(option)) {
                        state = 6;
                    } else {
                        throwPE(option, t.getName());
                    }
                    break;
                case 6:
                    if ("autoupdate".equalsIgnoreCase(option)) {
                        state = 7;
                        t.setAutoUpdate(false);
                    } else {
                        throwPE(option, t.getName());
                    }
                    break;
                case 7:
                    throwPE(option, t.getName());
                    break;
                default:
                    break;
            }
        if (!(state == 0 || state == 5 || state == 7)) {
            throwPE("", t.getName());
        }
    }

    private static void throwPE(String option, String tableName) throws ParseException {
        throw new ParseException(
                String.format(
                        "Invalid option for table '%s': '%s'. 'READ ONLY', "
                                + "'VERSION CHECK', 'NO VERSION CHECK', and/or 'NO AUTOUPDATE' expected.",
                        tableName, option));
    }

    private static void updateFK(Element fk, Table t) throws ParseException {
        String toSchema = fk.getAttribute("to_schema");
        String toTable = fk.getAttribute("to_table");
        String name = fk.getAttribute("name");

        Table referencedTable = t.getGrain().getScore().getGrain(toSchema).getTable(toTable);

        NodeList l = fk.getChildNodes();
        List<String> columns = new ArrayList<>(l.getLength());
        for (int i = 0; i < l.getLength(); i++) {
            Node n = l.item(i);
            if ("fk_column".equals(n.getNodeName())) {
                Element fkColumn = (Element) n;
                columns.add(fkColumn.getAttribute("name"));
            }
        }
        ForeignKey fkey = new ForeignKey(t, referencedTable, columns.toArray(new String[0]));

        if (!name.isEmpty())
            fkey.setConstraintName(name);

        String deleteRule = fk.getAttribute("delete_action");
        if ("cascade".equals(deleteRule)) {
            fkey.setDeleteRule(FKRule.CASCADE);
        } else if ("setNull".equals(deleteRule)) {
            fkey.setDeleteRule(FKRule.SET_NULL);
        }

        String updateRule = fk.getAttribute("update_action");
        if ("cascade".equals(updateRule)) {
            fkey.setUpdateRule(FKRule.CASCADE);
        } else if ("setNull".equals(updateRule)) {
            fkey.setUpdateRule(FKRule.SET_NULL);
        }
    }

    private static void updateIndex(Element index, Table t) throws ParseException {
        NodeList l = index.getChildNodes();
        String name = index.getAttribute("name");

        List<String> columns = new ArrayList<>(l.getLength());
        String celestaDoc = null;
        for (int i = 0; i < l.getLength(); i++) {
            Node n = l.item(i);
            if ("column".equals(n.getNodeName())) {
                Element column = (Element) n;
                columns.add(column.getAttribute("name"));
            } else if ("comment".equals(n.getNodeName()))
                celestaDoc = extractComment((Element) n);
        }

        if ("PRIMARY_KEY".equals(index.getAttribute("unique"))) {
            t.setPK(columns.toArray(new String[0]));
            t.setPkConstraintName(name);
        } else {
            Index idx = new Index(t, name, columns.toArray(new String[0]));
            idx.setCelestaDoc(celestaDoc);
        }

    }

    private static void updateColumn(Element column, Table t) throws ParseException {
        String celestaType = column.getAttribute("type");
        String columnName = column.getAttribute("name");
        boolean isNullable = !"y".equals(column.getAttribute("mandatory"));

        String celestaDoc = null;
        String defaultVal = null;
        NodeList l = column.getChildNodes();
        for (int i = 0; i < l.getLength(); i++) {
            Node n = l.item(i);
            if ("comment".equals(n.getNodeName())) {
                celestaDoc = extractComment((Element) n);
            } else if ("defo".equals(n.getNodeName())) {
                defaultVal = n.getTextContent();
            }
        }
        if (BinaryColumn.CELESTA_TYPE.equalsIgnoreCase(celestaType)) {
            BinaryColumn bc = new BinaryColumn(t, columnName);
            bc.setNullableAndDefault(isNullable, defaultVal);
            bc.setCelestaDoc(celestaDoc);
        } else if (BooleanColumn.CELESTA_TYPE.equalsIgnoreCase(celestaType)) {
            BooleanColumn bc = new BooleanColumn(t, columnName);
            bc.setNullableAndDefault(isNullable, defaultVal);
            bc.setCelestaDoc(celestaDoc);
        } else if (DateTimeColumn.CELESTA_TYPE.equalsIgnoreCase(celestaType)) {
            DateTimeColumn dc = new DateTimeColumn(t, columnName);
            dc.setNullableAndDefault(isNullable, defaultVal);
            dc.setCelestaDoc(celestaDoc);
        } else if (FloatingColumn.CELESTA_TYPE.equalsIgnoreCase(celestaType)) {
            FloatingColumn fc = new FloatingColumn(t, columnName);
            fc.setNullableAndDefault(isNullable, defaultVal);
            fc.setCelestaDoc(celestaDoc);
        } else if (IntegerColumn.CELESTA_TYPE.equalsIgnoreCase(celestaType)) {
            IntegerColumn ic = new IntegerColumn(t, columnName);
            if ("y".equals(column.getAttribute("autoincrement"))) {
                ic.setNullableAndDefault(isNullable, "IDENTITY");
            } else {
                ic.setNullableAndDefault(isNullable, defaultVal);
            }
            ic.setCelestaDoc(celestaDoc);
        } else if (StringColumn.VARCHAR.equalsIgnoreCase(celestaType)) {
            StringColumn sc = new StringColumn(t, columnName);
            sc.setLength(column.getAttribute("length"));
            sc.setNullableAndDefault(isNullable, defaultVal);
            sc.setCelestaDoc(celestaDoc);
        } else if ((StringColumn.TEXT).equalsIgnoreCase(celestaType)) {
            StringColumn sc = new StringColumn(t, columnName);
            sc.setLength("MAX");
            sc.setNullableAndDefault(isNullable, defaultVal);
            sc.setCelestaDoc(celestaDoc);
        }
    }
}

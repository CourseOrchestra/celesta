package ru.curs.celesta.dbschemasync;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import ru.curs.celesta.score.BinaryColumn;
import ru.curs.celesta.score.BooleanColumn;
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

/**
 * Переносит данные из DBSchema в Celesta.
 */
public final class DBSchema2Celesta {

	private static final Pattern VERSION = Pattern.compile(
			"version *('[^']+') *;", Pattern.CASE_INSENSITIVE);

	private DBSchema2Celesta() {
	}

	/**
	 * Преобразует DBS в Score.
	 * 
	 * @param dbs
	 *            DBS-файл
	 * @param refScore
	 *            score.
	 * @throws Exception
	 *             любая ошибка.
	 */
	public static void dBSToScore(File dbs, Score refScore) throws Exception {
		DocumentBuilderFactory docFactory = DocumentBuilderFactory
				.newInstance();
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
			}
		}

		refScore.save();
	}

	private static void updateGrainFK(Element schema, Grain g)
			throws ParseException {
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
			}
		}
	}

	private static void updateTableFK(Element table, Table t)
			throws ParseException {
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
			}
		}
	}

	private static void updateFK(Element fk, Table t) throws ParseException {
		String toSchema = fk.getAttribute("to_schema");
		String toTable = fk.getAttribute("to_table");

		Table referencedTable = t.getGrain().getScore().getGrain(toSchema)
				.getTable(toTable);

		NodeList l = fk.getChildNodes();
		List<String> columns = new ArrayList<>(l.getLength());
		for (int i = 0; i < l.getLength(); i++) {
			Node n = l.item(i);
			if ("fk_column".equals(n.getNodeName())) {
				Element fkColumn = (Element) n;
				columns.add(fkColumn.getAttribute("name"));
			}
		}
		ForeignKey fkey = new ForeignKey(t, referencedTable,
				columns.toArray(new String[0]));

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

	private static void updateIndex(Element index, Table t)
			throws ParseException {
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

	private static void updateColumn(Element column, Table t)
			throws ParseException {
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
				defaultVal = ((Element) n).getTextContent();
			}
		}
		if (BinaryColumn.CELESTA_TYPE.equals(celestaType)) {
			BinaryColumn bc = new BinaryColumn(t, columnName);
			bc.setNullableAndDefault(isNullable, defaultVal);
			bc.setCelestaDoc(celestaDoc);
		} else if (BooleanColumn.CELESTA_TYPE.equals(celestaType)) {
			BooleanColumn bc = new BooleanColumn(t, columnName);
			bc.setNullableAndDefault(isNullable, defaultVal);
			bc.setCelestaDoc(celestaDoc);
		} else if (DateTimeColumn.CELESTA_TYPE.equals(celestaType)) {
			DateTimeColumn dc = new DateTimeColumn(t, columnName);
			dc.setNullableAndDefault(isNullable, defaultVal);
			dc.setCelestaDoc(celestaDoc);
		} else if (FloatingColumn.CELESTA_TYPE.equals(celestaType)) {
			FloatingColumn fc = new FloatingColumn(t, columnName);
			fc.setNullableAndDefault(isNullable, defaultVal);
			fc.setCelestaDoc(celestaDoc);
		} else if (IntegerColumn.CELESTA_TYPE.equals(celestaType)) {
			IntegerColumn ic = new IntegerColumn(t, columnName);
			if ("y".equals(column.getAttribute("autoincrement"))) {
				ic.setNullableAndDefault(isNullable, "IDENTITY");
			} else {
				ic.setNullableAndDefault(isNullable, defaultVal);
			}
			ic.setCelestaDoc(celestaDoc);
		} else if (StringColumn.VARCHAR.equals(celestaType)) {
			StringColumn sc = new StringColumn(t, columnName);
			sc.setLength(column.getAttribute("length"));
			sc.setNullableAndDefault(isNullable, defaultVal);
			sc.setCelestaDoc(celestaDoc);
		} else if ((StringColumn.TEXT).equals(celestaType)) {
			StringColumn sc = new StringColumn(t, columnName);
			sc.setLength("MAX");
			sc.setNullableAndDefault(isNullable, defaultVal);
			sc.setCelestaDoc(celestaDoc);
		}
	}
}

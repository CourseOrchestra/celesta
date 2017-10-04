package ru.curs.celesta.dbutils.h2;

import org.h2.api.Trigger;
import ru.curs.celesta.Celesta;
import ru.curs.celesta.CurrentCelesta;
import ru.curs.celesta.event.TriggerType;
import ru.curs.celesta.score.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Created by ioann on 07.07.2017.
 */
abstract public class AbstractMaterializeViewTrigger implements Trigger {

  private static final Map<Integer, TriggerType> TRIGGER_TYPE_MAP = new HashMap<>();

  static {
    TRIGGER_TYPE_MAP.put(1, TriggerType.POST_INSERT);
    TRIGGER_TYPE_MAP.put(2, TriggerType.POST_UPDATE);
    TRIGGER_TYPE_MAP.put(4, TriggerType.POST_DELETE);
  }

  private Table t;
  private MaterializedView mv;

  private String tFullName;
  private String mvFullName;
  private String mvGroupByColumns;
  private String mvAllColumns;
  private HashMap<Integer, String> tGroupByColumnIndices = new LinkedHashMap<>();
  private HashMap<Integer, String> mvColumnRefs = new LinkedHashMap<>();

  @Override
  public void init(Connection connection, String schemaName, String triggerName, String tableName,
                   boolean before, int type) throws SQLException {

    try {
      Map<String, Grain> grains = CurrentCelesta.get().getScore().getGrains();
      Grain g = grains.get(schemaName);
      t = g.getElement(tableName, Table.class);

      mv = g.getElements(MaterializedView.class).values().stream()
          .filter(mv -> triggerName.equals(mv.getTriggerName(TRIGGER_TYPE_MAP.get(type))))
          .findFirst().get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    tFullName = String.format("\"%s\".\"%s\"", t.getGrain().getName(), t.getName());
    mvFullName = String.format("\"%s\".\"%s\"", mv.getGrain().getName(), mv.getName());

    mvAllColumns = mv.getColumns().keySet().stream()
        .filter(alias -> !MaterializedView.SURROGATE_COUNT.equals(alias))
        .map(v -> "\"" + v + "\"")
        .collect(Collectors.joining(", "));

    mvGroupByColumns = mv.getColumns().keySet().stream()
        .filter(alias -> mv.isGroupByColumn(alias))
        .map(alias -> "\"" + alias + "\"")
        .collect(Collectors.joining(", "));

    List<String> columnRefNames = mv.getColumnRefNames();

    int curIndex = 0;
    for (String tCol : t.getColumns().keySet()) {
      boolean isGroupBy = mv.getColumns().keySet().stream()
          .anyMatch(v -> mv.isGroupByColumn(v) && tCol.equals(mv.getColumnRef(v).getName()));

      if (isGroupBy) {
        tGroupByColumnIndices.put(curIndex, tCol);
      }
      
      if (columnRefNames.contains(tCol)) {
        mvColumnRefs.put(curIndex, tCol);
      }
      ++curIndex;
    }
  }


  void delete(Connection conn, Object[] row) throws SQLException {
    StringBuilder deleteSqlBuilder = new StringBuilder("DELETE FROM %s WHERE (%s) IN ");

    HashMap<String, Object> groupByColumnValues = getTableRowGroupByColumns(row);

    deleteSqlBuilder.append("(")
        .append(
            groupByColumnValues.entrySet().stream()
                .map(v -> {
                  try {
                    Column colRef = t.getColumn(v.getKey());

                    if (DateTimeColumn.CELESTA_TYPE.equals(colRef.getCelestaType()))
                      return "TRUNC(?)";

                    return "?";
                  } catch (ParseException e) {
                    throw new RuntimeException(e);
                  }
                })
                .collect(Collectors.joining(","))
        )
        .append(")");

    String deleteSql = String.format(deleteSqlBuilder.toString(), mvFullName, mvGroupByColumns);

    //System.out.println(deleteSql);
    PreparedStatement stmt = conn.prepareStatement(deleteSql);

    try {
      for (int i = 0; i < groupByColumnValues.size(); ++i) {
        stmt.setObject(i + 1, groupByColumnValues.values().toArray()[i]);
      }

      stmt.execute();
    } finally {
      stmt.close();
    }
  }

  void insert(Connection conn, Object[] row) throws SQLException {
    StringBuilder insertSqlBuilder = new StringBuilder("INSERT INTO %s (%s) %s");

    HashMap<String, Object> groupByColumnValues = getTableRowGroupByColumns(row);

    String whereCondition = groupByColumnValues.keySet().stream()
        .map(alias -> {
          try {
            Column colRef = t.getColumn(alias);

            if (DateTimeColumn.CELESTA_TYPE.equals(colRef.getCelestaType()))
              return "TRUNC(\"" + alias + "\") = TRUNC(?)";

            return "\"" + alias + "\" = ?";
          } catch(ParseException e) {
            throw new RuntimeException(e);
          }
        })
        .collect(Collectors.joining(" AND "));

    String selectPartOfScript = mv.getColumns().keySet().stream()
        .filter(alias -> !MaterializedView.SURROGATE_COUNT.equals(alias))
        .map(alias -> {
          Column colRef = mv.getColumnRef(alias);

          Map<String, Expr> aggrCols = mv.getAggregateColumns();
          if (aggrCols.containsKey(alias)) {
            if (colRef == null) {
              if (aggrCols.get(alias) instanceof Count) {
                return "COUNT(*) as \"" + alias + "\"";
              }
              return "";
            } else if (aggrCols.get(alias) instanceof Sum) {
              return "SUM(\"" + colRef.getName() + "\") as \"" + alias + "\"";
            } else {
              return "";
            }
          }

          if (DateTimeColumn.CELESTA_TYPE.equals(colRef.getCelestaType())) {
            return "TRUNC(\"" + colRef.getName() + "\") as \"" + alias + "\"";
          }

          return "\"" + colRef.getName() + "\" as " + "\"" + alias + "\"";
        })
        .filter(str -> !str.isEmpty())
        .collect(Collectors.joining(", "))
        .concat(", COUNT(*) AS " + MaterializedView.SURROGATE_COUNT);

          StringBuilder selectStmtBuilder = new StringBuilder("SELECT ")
              .append(selectPartOfScript)
              .append(" FROM ").append(tFullName).append(" ");
          selectStmtBuilder.append(" WHERE ").append(whereCondition)
              .append(mv.getGroupByPartOfScript());

          String insertSql = String.format(insertSqlBuilder.toString(), mvFullName,
              mvAllColumns + ", \"" + MaterializedView.SURROGATE_COUNT + "\"", selectStmtBuilder.toString());

          PreparedStatement stmt = conn.prepareStatement(insertSql);
          //System.out.println(insertSql);

          try {
            for (int i = 0; i < groupByColumnValues.size(); ++i) {
              stmt.setObject(i + 1, groupByColumnValues.values().toArray()[i]);
            }

            //System.out.println(stmt.toString());
            stmt.execute();
          } finally {
            stmt.close();
          }
        }


    @Override
    public void close () throws SQLException {
    }

    @Override
    public void remove () throws SQLException {
    }

  abstract String getNamePrefix();

  private HashMap<String, Object> getTableRowGroupByColumns(Object[] row) {
    HashMap<String, Object> result = new LinkedHashMap<>();

    tGroupByColumnIndices.entrySet().stream()
        .forEach(e -> result.put(e.getValue(), row[e.getKey()]));

    return result;
  }

  HashMap<Integer, String> getMvColumnRefs() {
    return mvColumnRefs;
  }
}

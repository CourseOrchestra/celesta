package ru.curs.celesta.dbutils.h2;

import org.h2.api.Trigger;
import ru.curs.celesta.Celesta;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.MaterializedView;
import ru.curs.celesta.score.Table;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Created by ioann on 07.07.2017.
 */
abstract public class AbstractMaterializeViewTrigger implements Trigger {

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
      Map<String, Grain> grains = Celesta.getInstance().getScore().getGrains();
      t = grains.get(schemaName).getTable(tableName);

      String mvStringToParse = triggerName.replace(getNamePrefix() + schemaName + "_" + tableName + "To", "");
      String[] mvSplittedFullName = mvStringToParse.split("_");
      String mvSchema = mvSplittedFullName[0];
      String mvName = mvSplittedFullName[1];

      mv = grains.get(mvSchema).getMaterializedView(mvName);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    tFullName = String.format("\"%s\".\"%s\"", t.getGrain().getName(), t.getName());
    mvFullName = String.format("\"%s\".\"%s\"", mv.getGrain().getName(), mv.getName());

    mvAllColumns = mv.getColumns().keySet().stream()
        .map(v -> "\"" + v + "\"")
        .collect(Collectors.joining(", "));

    mvGroupByColumns = mv.getColumns().keySet().stream()
        .filter(alias -> mv.isGroupByColumn(alias))
        .map(alias -> "\"" + alias + "\"")
        .collect(Collectors.joining(", "));

    List<String> columnRefNames = mv.getColumnRefNames();

    int curIndex = 0;
    for (String tCol : t.getColumns().keySet()) {
      if (mvGroupByColumns.contains(tCol)) {
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
        .append(groupByColumnValues.entrySet().stream().map(v -> "?").collect(Collectors.joining(",")))
        .append(")");

    String deleteSql = String.format(deleteSqlBuilder.toString(), mvFullName, mvGroupByColumns);

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
        .map(alias -> "\"" + alias + "\" = ?")
        .collect(Collectors.joining(" AND "));


    StringBuilder selectStmtBuilder = new StringBuilder(mv.getSelectPartOfScript())
        .append(" FROM ").append(tFullName).append(" ");
    selectStmtBuilder.append(" WHERE ").append(whereCondition)
        .append(mv.getGroupByPartOfScript());

    String insertSql = String.format(insertSqlBuilder.toString(), mvFullName,
        mvAllColumns, selectStmtBuilder.toString());

    PreparedStatement stmt = conn.prepareStatement(insertSql);

    try {
      for (int i = 0; i < groupByColumnValues.size(); ++i) {
        stmt.setObject(i + 1, groupByColumnValues.values().toArray()[i]);
      }

      stmt.execute();
    } finally {
      stmt.close();
    }
  }


  @Override
  public void close() throws SQLException { }

  @Override
  public void remove() throws SQLException { }

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

package ru.curs.celesta.dbutils.h2;

import org.h2.api.Trigger;
import ru.curs.celesta.CurrentScore;
import ru.curs.celesta.event.TriggerType;
import ru.curs.celesta.score.AbstractScore;
import ru.curs.celesta.score.BasicTable;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.Count;
import ru.curs.celesta.score.DateTimeColumn;
import ru.curs.celesta.score.Expr;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.MaterializedView;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Sum;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Base class for all triggers of materialized view.
 *
 * @author ioann
 * @since 2017-07-07
 */
public abstract class AbstractMaterializeViewTrigger implements Trigger {

    private static final Map<Integer, TriggerType> TRIGGER_TYPE_MAP = new HashMap<>();

    static {
        TRIGGER_TYPE_MAP.put(1, TriggerType.POST_INSERT);
        TRIGGER_TYPE_MAP.put(2, TriggerType.POST_UPDATE);
        TRIGGER_TYPE_MAP.put(4, TriggerType.POST_DELETE);
    }

    private BasicTable t;
    private MaterializedView mv;

    private String tFullName;
    private String mvFullName;
    private String keySearchTerm;
    private String mvAllColumns;
    private final HashMap<Integer, String> tGroupByColumnIndices = new LinkedHashMap<>();
    private final HashMap<Integer, String> mvColumnRefs = new LinkedHashMap<>();

    @Override
    public void init(Connection connection, String schemaName, String triggerName, String tableName,
                     boolean before, int type) {

        try {
            AbstractScore score = CurrentScore.get();
            Map<String, Grain> grains = score.getGrains();
            Grain g = grains.get(schemaName);
            t = g.getElement(tableName, BasicTable.class);

            mv = g.getElements(MaterializedView.class).values().stream()
                    .filter(view -> triggerName.equals(view.getTriggerName(TRIGGER_TYPE_MAP.get(type))))
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

        List<String> keySearchTerms = new ArrayList<>();
        List<String> columnRefNames = mv.getColumnRefNames();
        int curIndex = 0;
        for (String tCol : t.getColumns().keySet()) {
            for (Map.Entry<String, Column<?>> col : mv.getColumns().entrySet()) {
                if (mv.isGroupByColumn(col.getKey()) && tCol.equals(mv.getColumnRef(col.getKey()).getName())) {
                    tGroupByColumnIndices.put(curIndex, tCol);
                    keySearchTerms.add(String.format("(\"%s\" = %s)",
                            col.getKey(),
                            DateTimeColumn.CELESTA_TYPE.equals(
                                    col.getValue().getCelestaType()) ? "DATE_TRUNC(DAY, CAST(? AS TIMESTAMP))" : "?"));
                    break;
                }
            }

            if (columnRefNames.contains(tCol)) {
                mvColumnRefs.put(curIndex, tCol);
            }
            ++curIndex;
        }
        keySearchTerm = String.join(" and ", keySearchTerms);
    }


    final void delete(Connection conn, Object[] row) throws SQLException {

        HashMap<String, Object> groupByColumnValues = getTableRowGroupByColumns(row);

        String deleteSql = String.format("DELETE FROM %s WHERE %s", mvFullName, keySearchTerm);

        setParamsAndRun(conn, groupByColumnValues, deleteSql);
    }

    final void insert(Connection conn, Object[] row) throws SQLException {

        HashMap<String, Object> groupByColumnValues = getTableRowGroupByColumns(row);

        String whereCondition = groupByColumnValues.keySet().stream()
                .map(alias -> {
                    try {
                        return DateTimeColumn.CELESTA_TYPE.equals(
                                t.getColumn(alias).getCelestaType())
                                ? "DATE_TRUNC(DAY, \"" + alias + "\") = DATE_TRUNC(DAY, CAST(? AS TIMESTAMP))"
                                : "\"" + alias + "\" = ?";
                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.joining(" AND "));


        String selectPartOfScript = mv.getColumns().keySet().stream()
                .filter(alias -> !MaterializedView.SURROGATE_COUNT.equals(alias))
                .map(alias -> {
                    Column<?> colRef = mv.getColumnRef(alias);

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
                        return "DATE_TRUNC(DAY, \"" + colRef.getName() + "\") as \"" + alias + "\"";
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

        String insertSql = String.format("INSERT INTO %s (%s) %s", mvFullName,
                mvAllColumns + ", \"" + MaterializedView.SURROGATE_COUNT + "\"", selectStmtBuilder);


        setParamsAndRun(conn, groupByColumnValues, insertSql);
    }

    private void setParamsAndRun(
            Connection conn, HashMap<String, Object> groupByColumnValues, String insertSql) throws SQLException {

        try (PreparedStatement stmt = conn.prepareStatement(insertSql)) {
            int i = 0;
            for (Object value : groupByColumnValues.values()) {
                stmt.setObject(++i, value);
            }
            stmt.execute();
        }
    }

    @Override
    public void close() {
        //nothing to do
    }

    @Override
    public void remove() {
        //nothing to do
    }

    abstract String getNamePrefix();

    private HashMap<String, Object> getTableRowGroupByColumns(Object[] row) {
        HashMap<String, Object> result = new LinkedHashMap<>();

        tGroupByColumnIndices.forEach((key, value) -> result.put(value, row[key]));

        return result;
    }

    final HashMap<Integer, String> getMvColumnRefs() {
        return mvColumnRefs;
    }
}

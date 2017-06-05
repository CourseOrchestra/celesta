package ru.curs.celesta.dbutils.term;

import ru.curs.celesta.AppSettings;
import ru.curs.celesta.Celesta;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.dbutils.filter.In;
import ru.curs.celesta.dbutils.filter.value.FieldsLookup;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;
import ru.curs.celesta.score.Table;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by ioann on 01.06.2017.
 */
public final class InTerm extends WhereTerm {

  private final In filter;

  public InTerm(In filter) {
    this.filter = filter;
  }

  @Override
  public String getWhere() throws CelestaException {

    DBAdaptor db = DBAdaptor.getAdaptor();
    AppSettings.DBType dbType = AppSettings.getDBType();
    final String template;

    switch (dbType) {
      case H2:
        template = "( %s ) IN (SELECT ( %s ) FROM %s )";
        break;
      case POSTGRES:
      case ORACLE:
        template = "( %s ) IN (SELECT %s FROM %s )";
        break;
      case MSSQL:
        template = ("EXISTS (SELECT 1 FROM %s WHERE %s)");
        break;
      default:
        throw new CelestaException("Unsupported dbType: " + dbType);
    }


    FieldsLookup lookup = filter.getLookup();

    if (AppSettings.DBType.MSSQL.equals(dbType)) {
      Table table = lookup.getCursor().meta();
      String tableStr = String.format(db.tableTemplate(), table.getGrain().getName(), table.getName());
      Table otherTable = lookup.getOtherCursor().meta();
      String otherTableStr = String.format(db.tableTemplate(), otherTable.getGrain().getName(), otherTable.getName());

      StringBuilder sb = new StringBuilder();

      List<String> fields = lookup.getFields();
      List<String> otherFields = lookup.getOtherFields();

      for (int i = 0; i < fields.size(); ++i) {
        sb.append(tableStr).append(".\"").append(fields.get(i)).append("\"")
            .append(" = ")
            .append(otherTableStr).append(".\"").append(otherFields.get(i)).append("\"");

        if (i + 1 != fields.size()) {
          sb.append(" AND ");
        }
      }

      String result = String.format(template, otherTableStr, sb.toString());
      return result;
    }

    String fieldsStr = String.join(",",
        lookup.getFields().stream()
            .map(s -> "\"" + s + "\"")
            .collect(Collectors.toList())
    );
    String otherFieldsStr = String.join(",",
        lookup.getOtherFields().stream()
            .map(s -> "\"" + s + "\"")
            .collect(Collectors.toList())
    );

    Table otherTable = lookup.getOtherCursor().meta();
    String otherTableStr = String.format(db.tableTemplate(), otherTable.getGrain().getName(), otherTable.getName());

    String result = String.format(template, fieldsStr, otherFieldsStr, otherTableStr);
    return result;
  }

  @Override
  public void programParams(List<ParameterSetter> program) throws CelestaException {
    //DO NOTHING
  }
}
